from datetime import datetime
from airflow.decorators import dag, task
from pyspark.sql import SparkSession

# Define default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

# Define the DAG using the @dag decorator
@dag(
    dag_id="spark_postgres_to_hdfs",
    default_args=default_args,
    description="Load data from PostgreSQL to HDFS using Spark",
    schedule_interval="0 12 * * *",  # Daily at 12:00 PM
    start_date=datetime(2023, 12, 6),
    catchup=False,
)
def spark_postgres_to_hdfs_dag():
    # Define a PySpark task using the @task.pyspark decorator
    @task.pyspark(
        conn_id="spark_default",
        task_id="load_data_to_hdfs",
        config_kwargs={
            "spark.jars": "/opt/airflow/dags/postgresql-42.2.6.jar",  # Path to the PostgreSQL JDBC driver
        },
    )
    def load_data_to_hdfs(spark: SparkSession):
        # PostgreSQL configuration
        pg_config = {
            "url": "jdbc:postgresql://10.244.0.234:5432/banking_data",
            "user": "ps_user",
            "password": "thiemcun@169",
            "driver": "org.postgresql.Driver",
        }

        # Queries to fetch data
        data_queries = {
            "customers": """
                SELECT id, username, full_name, date_of_birth, gender, marital_status,
                       nationality, occupation, education_level, income, phone_number,
                       address, email, date_joined, loan_approval_status, loan_amount,
                       customer_segment, is_vip
                FROM public.customers
            """,
            "accounts": """
                SELECT account_id, customer_id, is_active, initial_balance, date_opened,
                       date_closed, current_balance
                FROM public.accounts
            """,
        }

        # HDFS base path
        hdfs_base_path = "hdfs://my-hdfs-namenode-0.my-hdfs-namenode.default.svc.cluster.local/user/data/banking_data/"

        for table_name, query in data_queries.items():
            try:
                # Read data from PostgreSQL
                df = (
                    spark.read.format("jdbc")
                    .option("url", pg_config["url"])
                    .option("dbtable", f"({query}) as {table_name}")
                    .option("user", pg_config["user"])
                    .option("password", pg_config["password"])
                    .option("driver", pg_config["driver"])
                    .load()
                )

                # Write data to HDFS in Parquet format
                hdfs_output_path = f"{hdfs_base_path}{table_name}"
                df.write.mode("overwrite").parquet(hdfs_output_path)

                print(f"Successfully written {table_name} data to {hdfs_output_path}")

            except Exception as e:
                print(f"Error processing {table_name}: {e}")

    # Define the task in the DAG
    load_data_to_hdfs()

# Instantiate the DAG
spark_postgres_to_hdfs_dag()