from pyspark.sql import SparkSession

from combinedmodule import data_preparation_api
from productmodule import product_data_collector_api
from salesmodule import sales_data_collector_api

if __name__ == '__main__':
    spark:SparkSession = SparkSession.builder.master("local[1]").appName("bootcamp.com").enableHiveSupport().getOrCreate()
    txt_file_path = "file:///home/takeo/pycharmprojects/sales.csv"
    txt2_file_path="file:///home/takeo/pycharmprojects/product.csv"
    sales_table="default.sales_data1"
    product_table= "default.product_data1"
    output_data="default.data_prep1"

    sales_data_collector_api(spark,txt_file_path,sales_table)
    product_data_collector_api(spark,txt2_file_path,product_table)
    data_preparation_api(spark,sales_table, product_table,output_data)
