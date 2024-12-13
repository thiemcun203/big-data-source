from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.types import *
from pyspark.sql.functions import udf
from pyspark.sql import DataFrame
import os
postgresql_jar_path = "/tmp/postgresql-42.2.6.jar"
os.environ["PYSPARK_SUBMIT_ARGS"] = f"--jars {postgresql_jar_path} pyspark-shell"

# --------------------------------------------------
# Spark Session Initialization
# --------------------------------------------------
spark = (SparkSession.builder
         .appName("BankingInsights")
         .config("spark.jars.packages", "org.postgresql:postgresql:42.2.5")  # Adjust version if needed
         .getOrCreate())

spark.sparkContext.setLogLevel("INFO")

# PostgreSQL connection config
pg_config = {
    "url": "jdbc:postgresql://postgres:5432/banking_data",
    "user": "ps_user",
    "password": "thiemcun@169",
    "driver": "org.postgresql.Driver"
}

# --------------------------------------------------
# Read Data from HDFS (Batch)
# --------------------------------------------------

# Schema inference or predefined schemas (optional)
customers_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("username", StringType(), True),
    StructField("full_name", StringType(), True),
    StructField("date_of_birth", DateType(), True),
    StructField("gender", StringType(), True),
    StructField("marital_status", StringType(), True),
    StructField("nationality", StringType(), True),
    StructField("occupation", StringType(), True),
    StructField("education_level", StringType(), True),
    StructField("income", DoubleType(), True),
    StructField("phone_number", StringType(), True),
    StructField("address", StringType(), True),
    StructField("email", StringType(), True),
    StructField("date_joined", DateType(), True),
    StructField("loan_approval_status", StringType(), True),
    StructField("loan_amount", DoubleType(), True),
    StructField("customer_segment", StringType(), True),
    StructField("is_vip", BooleanType(), True)
])

accounts_schema = StructType([
    StructField("account_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("is_active", BooleanType(), True),
    StructField("initial_balance", DoubleType(), True),
    StructField("date_opened", DateType(), True),
    StructField("date_closed", DateType(), True),
    StructField("current_balance", DoubleType(), True)
])

transactions_schema = StructType([
    StructField("transaction_id", IntegerType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("sender_account_id", IntegerType(), True),
    StructField("receiver_account_id", IntegerType(), True),
    StructField("amount", DoubleType(), True),
    StructField("transaction_fee", DoubleType(), True),
    StructField("subtype", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("location", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("product_amount", IntegerType(), True),
    StructField("transaction_message", StringType(), True)
])

customers_df = (spark.read.parquet("hdfs://my-hdfs-namenode-0.my-hdfs-namenode.default.svc.cluster.local/user/data/banking_data/customers/"))
                # .format("parquet")  # or parquet
                # .option("header", True)
                # .schema(customers_schema)
                # .load("hdfs://my-hdfs-namenode-0.my-hdfs-namenode.default.svc.cluster.local/user/data/banking_data/customers/"))

accounts_df = (spark.read.load("hdfs://my-hdfs-namenode-0.my-hdfs-namenode.default.svc.cluster.local/user/data/banking_data/accounts/"))
            #    .format("parquet")  # or parquet
            #    .option("header", True)
            #    .schema(accounts_schema)
            #    .load("hdfs://my-hdfs-namenode-0.my-hdfs-namenode.default.svc.cluster.local/user/data/banking_data/accounts/"))

# For transactions, we will handle streaming below
# But we can also do initial batch if needed
# transactions_df = (spark.read
#                    .format("csv")
#                    .option("header", True)
#                    .schema(transactions_schema)
#                    .load("hdfs://my-hdfs-namenode-0.my-hdfs-namenode.default.svc.cluster.local/user/data/banking_data/transactions/"))

# --------------------------------------------------
# Join & Optimization Strategies
# --------------------------------------------------
# Assume customers is small enough to broadcast
broadcast_customers_df = F.broadcast(customers_df)

# Join accounts with customers
cust_accounts_df = (accounts_df
                    .join(broadcast_customers_df, accounts_df.customer_id == broadcast_customers_df.id, "left")
                    .select("account_id", "customer_id", "full_name", "income", "customer_segment", "occupation", "is_active",
                            "current_balance", "date_opened", "is_vip"))

cust_accounts_df.cache()  # We will use it multiple times

# --------------------------------------------------
# Define a UDF for some custom logic (if needed)
# Example: Categorize customers based on income level
def income_category(income):
    if income is None:
        return "Unknown"
    elif income < 20000:
        return "Low"
    elif income < 70000:
        return "Medium"
    else:
        return "High"

income_category_udf = udf(income_category, StringType())
cust_accounts_df = cust_accounts_df.withColumn("income_category", income_category_udf(F.col("income")))

# --------------------------------------------------
# Structured Streaming from Transactions
# --------------------------------------------------
# Assume new transactions come in as JSON files
# transactions_stream_df = (spark.readStream
#                           .format("json")
#                           .schema(transactions_schema)
#                           .option("maxFilesPerTrigger", 1)  # For testing
#                           .load("hdfs://my-hdfs-namenode-0.my-hdfs-namenode.default.svc.cluster.local/user/data/banking_data/transactions/"))

# Join transactions with accounts to get customer info
# We will use a stateful join or static DataFrame join (static dimension join)
# Since accounts and customers are relatively static, we can join in memory.
# transactions_enriched = (transactions_stream_df
#                          .join(cust_accounts_df, transactions_stream_df.sender_account_id == cust_accounts_df.account_id, "left"))

# --------------------------------------------------
# Insight 1: Top 10 Customers by Transaction Volume (Batch Approach)
# --------------------------------------------------
# For demonstration, let's say we do a batch calculation from a historical snapshot.
# If needed, we can write a trigger-once streaming job or a static load before streaming.

# Suppose we read historical transactions (if available) for Insight 1
historical_transactions_df = (spark.read.parquet("hdfs://my-hdfs-namenode-0.my-hdfs-namenode.default.svc.cluster.local/user/data/banking_data/transactions/"))
                            #   .format("parquet")
                            #   .option("header", True)
                            #   .schema(transactions_schema)
                            #   .load("hdfs://my-hdfs-namenode-0.my-hdfs-namenode.default.svc.cluster.local/user/data/banking_data/transactions/"))

historical_enriched = (historical_transactions_df
                       .join(cust_accounts_df, historical_transactions_df.sender_account_id == cust_accounts_df.account_id, "left"))

top_customers_window = Window.orderBy(F.col("total_amount").desc())
top_customers_df = (historical_enriched
                    .groupBy("customer_id", "full_name")
                    .agg(F.sum("amount").alias("total_amount"))
                    .withColumn("rank", F.row_number().over(top_customers_window))
                    .filter(F.col("rank") <= 10)
                    .select("customer_id", "full_name", "total_amount", "rank"))

top_customers_df.write \
    .format("jdbc") \
    .option("url", pg_config["url"]) \
    .option("dbtable", "insight_top_customers") \
    .option("user", pg_config["user"]) \
    .option("password", pg_config["password"]) \
    .option("driver", pg_config["driver"]) \
    .mode("overwrite") \
    .save()
    
print("Customers DF:")
customers_df.show(5, truncate=False)

print("Accounts DF:")
accounts_df.show(5, truncate=False)

print("Historical Transactions DF:")
historical_transactions_df.show(5, truncate=False)

print("Historical Enriched DF:")
historical_enriched.show(5, truncate=False)

print("Top Customers DF:")
top_customers_df.show(5, truncate=False)
# --------------------------------------------------
# Insight 2: Monthly Transaction Volume by Type (Pivot)
# --------------------------------------------------
# monthly_type_df = (historical_enriched
#                    .withColumn("year_month", F.date_format("timestamp", "yyyy-MM"))
#                    .groupBy("year_month")
#                    .pivot("transaction_type")
#                    .agg(F.sum("amount").alias("total_amount"))
#                   )

# monthly_type_df.write \
#     .format("jdbc") \
#     .option("url", pg_config["url"]) \
#     .option("dbtable", "insight_monthly_type") \
#     .option("user", pg_config["user"]) \
#     .option("password", pg_config["password"]) \
#     .option("driver", pg_config["driver"]) \
#     .mode("overwrite") \
#     .save()

# --------------------------------------------------
# Insight 3: Customer Segmentation Analysis
# Average transaction amount by customer_segment and occupation
# We'll pivot by occupation to see differences across segments
# segmentation_df = (historical_enriched
#                    .groupBy("customer_segment")
#                    .pivot("occupation")
#                    .agg(F.avg("amount").alias("avg_amount"))
#                   )

# segmentation_df.write \
#     .format("jdbc") \
#     .option("url", pg_config["url"]) \
#     .option("dbtable", "insight_segmentation") \
#     .option("user", pg_config["user"]) \
#     .option("password", pg_config["password"]) \
#     .option("driver", pg_config["driver"]) \
#     .mode("overwrite") \
#     .save()

# --------------------------------------------------
# Insight 4: Geographical distribution and product pivot
# geo_product_df = (historical_enriched
#                   .groupBy("location")
#                   .pivot("product_name")
#                   .agg(F.sum("amount").alias("total_amount"))
#                  )

# geo_product_df.write \
#     .format("jdbc") \
#     .option("url", pg_config["url"]) \
#     .option("dbtable", "insight_geo_product") \
#     .option("user", pg_config["user"]) \
#     .option("password", pg_config["password"]) \
#     .option("driver", pg_config["driver"]) \
#     .mode("overwrite") \
#     .save()

# --------------------------------------------------
# Insight 5: Streaming: Hourly transaction counts and amount with watermarking
# --------------------------------------------------
# Add a watermark on timestamp to handle late data
# hourly_agg = (transactions_enriched
#               .withWatermark("timestamp", "30 minutes")
#               .groupBy(F.window("timestamp", "1 hour"), "transaction_type")
#               .agg(F.count("*").alias("tx_count"), F.sum("amount").alias("total_amount"))
#               .select(F.col("window.start").alias("hour_start"),
#                       F.col("window.end").alias("hour_end"),
#                       "transaction_type",
#                       "tx_count",
#                       "total_amount"))

# # Write the streaming result to PostgreSQL (micro-batch)
# # For demonstration, assume ForeachBatch to handle streaming to JDBC
# def write_to_postgres(batch_df, batch_id):
#     (batch_df.write
#      .format("jdbc")
#      .option("url", pg_config["url"])
#      .option("dbtable", "insight_hourly_stream")
#      .option("user", pg_config["user"])
#      .option("password", pg_config["password"])
#      .option("driver", pg_config["driver"])
#      .mode("append")
#      .save())

# query = (hourly_agg
#          .writeStream
#          .outputMode("update")  # update mode since we are doing aggregates
#          .foreachBatch(write_to_postgres)
#          .trigger(processingTime='1 minute')
#          .start())

# query.awaitTermination()

# --------------------------------------------------
# End of Code
# --------------------------------------------------

# Note: In a real environment, you might separate the batch insights job from the streaming job.
# Also, ensure that you have the correct permissions and network connectivity to Postgres.