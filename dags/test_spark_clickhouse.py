from pendulum import datetime, duration
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from lib.clickhouse_operator_extended import ClickHouseOperatorExtended
import logging
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
    description='testing connection',
    doc_md=__doc__
)
def spark_clickhouse_example():
    spark_submit_task = SparkSubmitOperator(
        task_id='spark_submit_job',
        application='dags/spark_app/spark_1.py',
        #conn_id='spark_master',
        conn_id='spark',
        total_executor_cores='1',
        executor_cores='1',
        executor_memory=f'{ram}g',
        num_executors='1',
        driver_memory=f'{ram}g',
        verbose=True
    )

    ch_list_count_rows_start_of_month = ClickHouseOperatorExtended(
        task_id='ch_list_count_rows_start_of_month',
        clickhouse_conn_id=CLICKHOUSE_CONN_ID,
        sql='test.sql'
    )
    
    spark_submit_task >> ch_list_count_rows_start_of_month # >> последовательно, | - паралельное выполнение

spark_clickhouse_example()



