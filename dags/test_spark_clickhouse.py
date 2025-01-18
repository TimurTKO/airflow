from pendulum import datetime, duration
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from lib.clickhouse_operator_extended import ClickHouseOperatorExtended
import logging
from airflow.decorators import dag, task
from pyspark.sql import SparkSession
from pyspark import SparkContext
import os 
PYSPARK_CONN_ID = 'spark'
CH_IP = os.getenv('CH_IP')
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD')

packages = [
        "com.clickhouse.spark:clickhouse-spark-runtime-3.5_2.12:0.8.1"
        ,"com.clickhouse:clickhouse-jdbc:0.7.0"
        ,"com.clickhouse:clickhouse-client:0.7.0"
        ,"com.clickhouse:clickhouse-http-client:0.7.0"
        ,"org.apache.httpcomponents.client5:httpclient5:5.3.1"
        #,'org.apache.sedona:sedona-spark-3.5_2.12:1.7.0'
        #,'org.datasyslab:geotools-wrapper:1.7.0-28.5'
        #,'uk.co.gresearch.spark:spark-extension_2.12:2.11.0-3.4'
    ]

CLICKHOUSE_CONN_ID = 'clickhouse'
default_args={
    "owner": "TKO",
    "depends_on_past": False, # if the past is failed, the next one will run
    "retries": 0 # if the task is failed, it will retry 0 times
}
ram = 12
cpu = 30*3
@dag(
    tags=["test", "stocks"],
    render_template_as_native_obj=True,  # render template as a python object
    max_active_runs=1,
    #schedule='50 2 * * *',
    schedule=None,
    default_args=default_args,
    start_date=datetime(2023, 12, 1),
    catchup=False,
    template_searchpath='dags',
    description='testing connection',
    doc_md=__doc__
)
def spark_clickhouse_example():
    spark_submit_task = SparkSubmitOperator(
        task_id='spark_submit_job',
        application='dags/spark_app/spark_1.py',
        #conn_id='spark_master',
        conn_id=PYSPARK_CONN_ID,
        #total_executor_cores='1',
        #executor_cores='1',
        #maxResultSize=f'{ram}g',
        #memoryOverhead=f'{ram}g',
        packages=','.join(packages),
        executor_memory=f'{ram}g',
        #num_executors='1',
        driver_memory=f'{ram}g',
        verbose=True
    )

    ch_list_count_rows_start_of_month = ClickHouseOperatorExtended(
        task_id='ch_list_count_rows_start_of_month',
        clickhouse_conn_id=CLICKHOUSE_CONN_ID,
        sql='include/test.sql'
    )

    @task.pyspark(
            conn_id=PYSPARK_CONN_ID
            ,config_kwargs={
                'spark.jars.packages':','.join(packages)
                ,'spark.executor.memory': f'{ram}g'
                ,'spark.driver.memory': f'{ram}g'
                ,"spark.driver.maxResultSize": f"{ram}g"
                ,"spark.executor.memoryOverhead": f"{ram}g"
            }
        )
    def get_files_and_unite(spark: SparkSession, sc: SparkContext):
        import requests
        spark = (
            SparkSession.builder
            .config('spark.jars.repositories', 'https://artifacts.unidata.ucar.edu/repository/unidata-all')
            .config("spark.sql.catalog.clickhouse", "com.clickhouse.spark.ClickHouseCatalog")
            .config("spark.sql.catalog.clickhouse.host", CH_IP)
            .config("spark.sql.catalog.clickhouse.protocol", "http")
            .config("spark.sql.catalog.clickhouse.http_port", "8123")
            .config("spark.sql.catalog.clickhouse.user", CLICKHOUSE_USER)
            .config("spark.sql.catalog.clickhouse.password", CLICKHOUSE_PASSWORD) 
            .config("spark.sql.catalog.clickhouse.database", "default")
            .config("spark.clickhouse.write.format", "json")
            .getOrCreate()
        )
        sc = spark.sparkContext
        spark.sql("use clickhouse")
        spark.sql("Select * from bank.salaryprediction_2").show()

        data = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]
        df = spark.createDataFrame(data, ["Name", "Value"])
        df.write.csv("/opt/airflow/test/df_test", mode="overwrite")
        df = spark.read.csv("/opt/airflow/test/df_test.csv", header=True, inferSchema=True) # inferSchema=True - автоматически определяет тип данных
        df.show()


    #show all folders in the current directory
    #print(os.listdir('/'))

        df.show()
        df.toPandas().to_csv("/opt/airflow/test/df_test1.csv", index=False)
        spark.stop()
        return
    
    [spark_submit_task, ch_list_count_rows_start_of_month] >> get_files_and_unite()  # >> последовательно, | - паралельное выполнение

spark_clickhouse_example()



