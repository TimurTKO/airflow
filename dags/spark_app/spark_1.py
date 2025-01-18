from pyspark.sql import SparkSession
import os


CH_IP = os.getenv('CH_IP')
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD')

def run():
    #spark = SparkSession.builder.appName("ExampleJob111").getOrCreate()
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



if __name__ == "__main__":
    run()