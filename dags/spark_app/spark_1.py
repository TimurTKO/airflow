from pyspark.sql import SparkSession
import os
def run():
    spark = SparkSession.builder.appName("ExampleJob111").getOrCreate()
    data = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]
    df = spark.createDataFrame(data, ["Name", "Value"])
    df.toPandas().to_csv("/opt/airflow/test/df_test.csv", index=False)
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