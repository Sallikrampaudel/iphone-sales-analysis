"""
Implement a Python function sales_data_collector_api that consumes sales data
from a text file with a header, processes it, and
publishes it into a partitioned Hive table in Parquet format.
This involves parsing the data, handling file headers, and partitioning by sale date.
"""
from pyspark.sql import SparkSession

def sales_data_collector_api(txt_path):
    # Initialize Spark session with Hive support enabled
    spark = SparkSession.builder \
        .appName("SalesDataCollector") \
        .enableHiveSupport() \
        .getOrCreate()

    # Set Hive dynamic partitioning configurations
    spark.conf.set("hive.exec.dynamic.partition", "true")
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

    # Read the text file
    df = spark.read.option("header", True) \
        .csv(txt_path)

    # Show the schema to ensure the data is loaded correctly
    df.printSchema()

    # Write the DataFrame to a Hive table, partitioned by 'sale_date' and stored as Parquet
    df.write.mode("overwrite") \
        .partitionBy("sale_date") \
        .format("parquet") \
        .saveAsTable("hive_test.sales_data")

    # Verify the data by querying from the Hive table
    tabDF = spark.sql("SELECT * FROM hive_test.sales_data")
    #For Local Testing Purpose
    print(tabDF.show())

if __name__ == '__main__':
    sales_data_collector_api("file:///home/takeo/sales_data.txt")