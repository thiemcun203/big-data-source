from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
from pyspark.sql import SparkSession

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 12),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# DAG definition
dag = DAG(
    'postgres_to_hdfs',
    default_args=default_args,
    description='Load data from PostgreSQL to HDFS',
    schedule_interval='@daily',
)

# PostgreSQL JDBC JAR path
postgresql_jar_path = "/opt/spark/jars/postgresql-42.2.6.jar"
os.environ["PYSPARK_SUBMIT_ARGS"] = f"--jars {postgresql_jar_path} pyspark-shell"

# PostgreSQL and HDFS configurations
pg_config = {
    "url": "jdbc:postgresql://postgres:5432/banking_data",
    "user": "ps_user",
    "password": "thiemcun@169",
    "driver": "org.postgresql.Driver"
}

data_queries = {
    "customers": "SELECT * FROM customers",
    "accounts": "SELECT * FROM accounts"
}

hdfs_path = "hdfs://my-hdfs-namenode-0.my-hdfs-namenode.default.svc.cluster.local/user/data/banking_data/"

def load_data_to_hdfs(ds, **kwargs):
    spark = SparkSession.builder \
        .appName("PostgresToHDFS") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://my-hdfs-namenode:8020") \
        .getOrCreate()

    for table_name, query in data_queries.items():
        df = spark.read \
            .format("jdbc") \
            .option("url", pg_config["url"]) \
            .option("dbtable", f"({query}) as {table_name}") \
            .option("user", pg_config["user"]) \
            .option("password", pg_config["password"]) \
            .option("driver", pg_config["driver"]) \
            .load()

        hdfs_output_path = os.path.join(hdfs_path, table_name)
        df.write.mode("overwrite").parquet(hdfs_output_path)

        print(f"Successfully written {table_name} data to {hdfs_output_path}")

    spark.stop()

# Task definitions
load_customers_task = PythonOperator(
    task_id='load_customers_data',
    python_callable=load_data_to_hdfs,
    op_kwargs={'table_name': 'customers'},
    dag=dag,
)

load_accounts_task = PythonOperator(
    task_id='load_accounts_data',
    python_callable=load_data_to_hdfs,
    op_kwargs={'table_name': 'accounts'},
    dag=dag,
)

# Setting task dependencies
load_customers_task >> load_accounts_task