
def product_data_collector_api(spark,txt2_file_path,product_table):
    df = spark.read.options(header='True', inferSchema='True', delimiter='|').csv(txt2_file_path)
    # spark.conf.set("hive.exec.dynamic.partition", "true")
    # spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    df1 = df.write.mode("overwrite").format("parquet").saveAsTable(product_table)
