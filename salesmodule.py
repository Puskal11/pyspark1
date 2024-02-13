
def sales_data_collector_api(spark,txt_file_path,sales_table):
    df = spark.read.options(header='True', inferSchema='True', delimiter='|').csv(txt_file_path)
    # spark.conf.set("hive.exec.dynamic.partition", "true")
    # spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    df1 = df.write.mode("overwrite").format("parquet").partitionBy("sales_date").saveAsTable(sales_table)
