"""
Implement a Python function data_preparation_api that consumes
data from the product and sales tables, processes it, and
generates the desired output into another Hive table. Specifically,
it should report the buyers who have bought the S8 but not the iPhone.
"""

from pyspark.sql import SparkSession


def data_preparation_api():
    spark = SparkSession.builder.appName("DataPreparation").enableHiveSupport().getOrCreate()

    sales_df = spark.sql("SELECT * FROM hive_test.sales_data")
    product_df = spark.sql("SELECT * FROM hive_test.product_data")

    s8_product_id = product_df.filter(product_df.product_name == 'S8').select('product_id').first()['product_id']
    sales_df.createOrReplaceTempView("sales_view")

    #Get the all the data with product id as s8_product_id
    buyers_s8 = spark.sql(f"SELECT DISTINCT buyer_id, product_id, seller_id, sale_date, quantity, price FROM sales_view WHERE product_id = {s8_product_id}")
    #for test
    #print(buyers_s8.show())

    buyers_s8.write.mode("overwrite").partitionBy("sale_date").format("parquet").saveAsTable("hive_test.s8_buyers")
    output=spark.sql("SELECT * FROM hive_test.s8_buyers")
    output.show()

    #For local testing
    #print(output.show())


if __name__ == '__main__':
    data_preparation_api()