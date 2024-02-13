from pyspark.sql import SparkSession
# def sales_data_collector_api(spark,file_path):
#     df = spark.read.options(header='True', inferSchema='True', delimiter='|').csv(file_path)
#     df1 = df.write.mode("overwrite").partitionBy("sales_date").saveAsTable("default.sales_data2")
# def product_data_collector_api(spark,file_path):
#     df = spark.read.options(header='True', inferSchema='True', delimiter='|').csv(file_path)
#     df1 = df.write.mode("overwrite").format("parquet").saveAsTable("default.product_data2")
def data_preparation_api(spark):
    df = spark.sql("SELECT buyer_id FROM sales_data2 WHERE product_id IN (SELECT product_id from product_data2 WHERE product_name='S8')")
    df.show()
    # df1 = df.write.mode("overwrite").saveAsTable("default.data_prep")
    # print(df1.printSchema)
    # print(df1.show())
if __name__ == '__main__':
    spark:SparkSession = SparkSession.builder.master("local[1]").appName("bootcamp.com").getOrCreate()

    # sales_data_collector_api(spark,"file:///home/takeo/pycharmprojects/sales.csv")
    # product_data_collector_api(spark,"file:///home/takeo/pycharmprojects/product.csv")
    data_preparation_api(spark)
