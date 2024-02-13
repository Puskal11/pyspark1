from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def dupEndpoints(spark,local_parquet_path):
    df = spark.read.parquet(local_parquet_path)
    df3=df.groupBy("endpoint").agg(sum("content_size").alias("Total_content_size"))
    df4=df3.limit(10).sort(col("Total_content_size").desc())
    df.show()
    df.count()
    print("Top 10 Endpoints which Transfer Maximum Content in KB")
    df4.show(truncate=False)

def top10visitEnpoint(spark,local_parquet_path):
    df = spark.read.parquet(local_parquet_path)
    df1 = df.groupBy("endpoint").agg(count("endpoint").alias("num_of_end_point"))
    df2=df1.sort(col("num_of_end_point").desc())
    print("Top 10 visited endpoints: ")
    df2.limit(10).show(truncate=False)


def daysNcontentSize(spark,local_parquet_path):
    df = spark.read.parquet(local_parquet_path)
    day_of_month= substring(col("date"),1,2)
    day_content_size_df = df.groupBy(day_of_month.alias('Day')).agg(sum('content_size').alias('visited_content_size'))
    print("List down the day and its visited content size: ")
    day_content_size_df.show()

def mincontentsize(spark,local_parquet_path):
    df= spark.read.parquet(local_parquet_path)
    df1 = df.select(min("content_size").alias("Minimum_Content_Size"))
    df2 = df1.sort(col("Minimum_Content_Size").desc())
    print("MINIMUM content size: ")
    df2.show(truncate=False)

def maxcontentsize(spark,local_parquet_path):
    df= spark.read.parquet(local_parquet_path)
    df1 = df.select(max("content_size").alias("Maximum_Content_Size"))
    df2=df1.sort(col("Maximum_Content_Size").desc())
    print("Maximum content size: ")
    df2.show(truncate=False)

def countcontentsize(spark,local_parquet_path):
    df= spark.read.parquet(local_parquet_path)
    df1 = df.select(count("content_size").alias("Number_of_Content_size"))
    df2=df1.sort(col("Number_of_Content_size").desc())
    print("Count Content size: ")
    df2.show(truncate=False)

def responsecodeAnalysis(spark,local_parquet_path):
    df= spark.read.parquet(local_parquet_path)
    # df3=df.groupBy("endpoint").agg(sum("content_size").alias("Total_content_size"))
    df1=df.groupBy("response_code").agg(count("response_code").alias("total_num_response"))
    print("Response Codes with Number of Codes: ")
    df1.show(truncate=False)

def ipaddressmorethan10(spark,local_parquet_path):
    df= spark.read.parquet(local_parquet_path)
    df1 = df.groupBy("ip_address").agg(count("*").alias("Number_of_Times_IP_Accessed"))
    print("Most Frequent Visitors (Most Frequent IP Address visits) which visited at-least 10 times: ")
    df1.sort(col("Count_content_size").desc()).limit(10).show(truncate=False)

def top10badrequest404errorendpointstime(spark):
    df2 = spark.sql("SELECT endpoint, time ,count(response_code) as num_of_resp FROM default.apilog_table where response_code in (select response_code from default.apilog_table where response_code= 404) Group By 1,2")
    print("Top 10 latest 404 requests with there endpoints and time: ")
    df2.limit(10).show(truncate=False)

if __name__ == '__main__':
    spark:SparkSession = SparkSession.builder.master("local[1]").appName("bootcamp.com").enableHiveSupport().getOrCreate()
    local_parquet_path="file:///home/takeo/pycharmprojects/apilogs.parquet"
    apilogs_table= "default.apilog_table"

    dupEndpoints(spark,local_parquet_path)
    top10visitEnpoint(spark,local_parquet_path)
    daysNcontentSize(spark,local_parquet_path)
    mincontentsize(spark,local_parquet_path)
    maxcontentsize(spark,local_parquet_path)
    countcontentsize(spark,local_parquet_path)
    responsecodeAnalysis(spark,local_parquet_path)
    top10badrequest404errorendpointstime(spark)

    # Question:
                #     Section 1
                # ●	Calculate statistics based on the content size
                # ○	Top 10 Endpoints which Transfer Maximum Content in KB
                # ○	Top 10 visited endpoints
                # ○	List down the day and its visited content size
                # ○	MIN content size
                # ○	MAX content size
                # ○	Count content size
                # Section 2
                # ●	Response Code Analysis
                # ○	Response Codes with Number of Codes
                # ●	Any IPAddress that has accessed the server more than 10 times
                # ○	Most Frequent Visitors (Most Frequent IP Address visits) which visited at-least 10 times.
                # Section 3
                # ●	Bad Request analysis
                # ○	Top 10 latest 404 requests with there endpoints and time
                # Submission
                #Pyspark_code
                #
                # Metadata
                # |-- ip_address: string (nullable = true)
                #  |-- client_identd: string (nullable = true)
                #  |-- user_id: string (nullable = true)
                #  |-- date: string (nullable = true)
                #  |-- time: string (nullable = true)
                #  |-- method: string (nullable = true)
                #  |-- endpoint: string (nullable = true)
                #  |-- protocol: string (nullable = true)
                #  |-- response_code: long (nullable = true)
                #  |-- content_size: long (nullable = true)






