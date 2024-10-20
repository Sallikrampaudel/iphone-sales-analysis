"""
Develop a Python function product_data_collector_api that consumes data from a Parquet file,
processes it, and publishes it into a non-partitioned Hive table.
This involves reading Parquet files and transferring data into a Hive table.
"""

from pyspark.sql import SparkSession

def product_data_collector(txt_path, path):

    #Initialize Spark Session with Hive Support Enabled
    spark = SparkSession.builder.appName("ProductDataCollector").enableHiveSupport().getOrCreate()

    #Read File
    df=spark.read.option("header", True).csv(txt_path)
    df.write.mode("overwrite").parquet("file:///home/takeo/parq/proudct_data.parquet")

    #read parquet file
    df=spark.read.parquet(path)
    #print(df.show())
    #df.printSchema()

    #Wirte DataFrame to Hive table, no partitioned
    df.write.mode("overwrite").format("parquet").saveAsTable("hive_test.product_data")

    # Verify the data by querying from the Hive table
    tabDF=spark.sql("SELECT * FROM hive_test.product_data")
    print(tabDF.show())

if __name__=='__main__':
    product_data_collector("file:///home/takeo/product.txt","file:///home/takeo/parq/proudct_data.parquet")
