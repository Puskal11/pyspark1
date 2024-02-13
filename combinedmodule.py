def data_preparation_api(spark, sales_table, product_table, output_data):
    df = spark.sql("""
        SELECT buyer_id
        FROM {}
        WHERE product_id IN
            (SELECT product_id
             from {}
             WHERE product_name='S8'
             )
    """.format(sales_table, product_table))
    print(df.printSchema)
    print(df.show())
    df1 = df.write.mode("overwrite").saveAsTable(output_data)
