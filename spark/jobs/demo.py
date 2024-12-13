from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_date, count, sum
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
import redis
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("FraudDetection")

# Define the schema for the Kafka messages
schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("sender_account_id", StringType(), True),
    StructField("receiver_account_id", StringType(), True),
    StructField("amount", DecimalType(10, 2), True),
    StructField("transaction_fee", DecimalType(10, 2), True),
    StructField("subtype", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("location", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("product_amount", DecimalType(10, 2), True),
    StructField("transaction_message", StringType(), True),
])

# Create a Spark session
spark = SparkSession.builder \
    .appName("KafkaFraudDetectionWithRedis") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .config("spark.dynamicAllocation.enabled", "true") \
    .getOrCreate()

# Kafka configuration
kafka_options = {
    "kafka.bootstrap.servers": "kafkadev-controller-headless:9092",
    "kafka.sasl.mechanism": "SCRAM-SHA-256",
    "kafka.security.protocol": "SASL_PLAINTEXT",
    "kafka.sasl.jaas.config": """org.apache.kafka.common.security.scram.ScramLoginModule required username="user1" password="N4yMeyCkX0";""",
    "subscribe": "test",
    "startingOffsets": "latest",  # Only process new data
    # Do NOT set "group.id"; let Spark manage consumer groups internally
}

# Read data from Kafka
raw_stream = spark.readStream \
    .format("kafka") \
    .options(**kafka_options) \
    .load()

# Deserialize the Kafka message value
messages = raw_stream.selectExpr("CAST(value AS STRING) as value")

# Parse JSON messages
parsed_messages = messages.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Convert timestamp field to TimestampType and extract date
parsed_messages = parsed_messages.withColumn("timestamp", col("timestamp").cast(TimestampType()))
parsed_messages = parsed_messages.withColumn("transaction_date", to_date(col("timestamp")))

# Filter data to ensure proper structure
filtered_messages = parsed_messages.filter(
    col("transaction_id").isNotNull() &
    col("sender_account_id").isNotNull() &
    col("amount").isNotNull() &
    col("timestamp").isNotNull()
)

# Aggregate data: Count transactions and sum amounts per user per day
aggregated_data = filtered_messages.groupBy(
    "sender_account_id", "transaction_date"
).agg(
    count("*").alias("daily_transaction_count"),
    sum("amount").alias("daily_transaction_amount")
)

# Define thresholds for fraud detection
TRANSACTION_COUNT_THRESHOLD = 3
TRANSACTION_AMOUNT_THRESHOLD = 10000.0

# Function to interact with Redis for fraud detection
def detect_fraud_and_store(batch_df, batch_id):
    logger.info(f"Processing batch {batch_id}")
    
    if batch_df.rdd.isEmpty():
        logger.info("Batch is empty. Skipping processing.")
        return
    
    try:
        # Connect to Redis
        redis_client = redis.StrictRedis(
            host="redis-master",
            port=6379,
            password="xts7qvg3yU",
            decode_responses=True
        )
        # Test Redis connection
        redis_client.ping()
        logger.info("Connected to Redis successfully.")
    except Exception as e:
        logger.error(f"Failed to connect to Redis: {e}")
        return
    
    try:
        # Iterate over rows
        for row in batch_df.collect():
            logger.info(f"Processing row: {row}")
            sender_account_id = row["sender_account_id"]
            transaction_date = row["transaction_date"]
            daily_transaction_count = row["daily_transaction_count"]
            daily_transaction_amount = float(row["daily_transaction_amount"]) if row["daily_transaction_amount"] is not None else 0.0
            
            # Check fraud conditions
            is_fraud = (
                daily_transaction_count > TRANSACTION_COUNT_THRESHOLD or
                daily_transaction_amount > TRANSACTION_AMOUNT_THRESHOLD
            )
            logger.info(f"Fraud status for {sender_account_id}: {is_fraud}")
            
            # Store in Redis
            redis_key = f"user_activity:{sender_account_id}:{transaction_date}"
            try:
                redis_client.hset(redis_key, mapping={
                    "daily_transaction_count": daily_transaction_count,
                    "daily_transaction_amount": daily_transaction_amount,
                    "is_fraud": is_fraud
                })
                logger.info(f"Stored data in Redis with key: {redis_key}")
            except Exception as e:
                logger.error(f"Failed to write to Redis for key {redis_key}: {e}")
            
            if is_fraud:
                logger.warning(f"Fraud detected for {sender_account_id} on {transaction_date}: "
                               f"Count={daily_transaction_count}, Amount={daily_transaction_amount}")
    except Exception as e:
        logger.error(f"Error processing batch {batch_id}: {e}")

# Define a unique checkpoint directory
checkpoint_dir = "/tmp/spark_checkpoint_kafka_fraud_detection"

# Ensure the checkpoint directory is clean (remove if exists)
if os.path.exists(checkpoint_dir):
    import shutil
    logger.info(f"Removing existing checkpoint directory at {checkpoint_dir}")
    shutil.rmtree(checkpoint_dir)

# Write the stream to Redis using foreachBatch and add a console sink for debugging
query_redis = aggregated_data.writeStream \
    .outputMode("update") \
    .foreachBatch(detect_fraud_and_store) \
    .option("checkpointLocation", checkpoint_dir) \
    .start()

query_console = aggregated_data.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .option("checkpointLocation", checkpoint_dir + "_console") \
    .start()

# Wait for the streaming to finish
spark.streams.awaitAnyTermination()