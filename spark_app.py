from pyspark.sql import SparkSession

def top5category(spark,file_path):
     rdd = spark.sparkContext.textFile(file_path)
     rdd1 = rdd.map(lambda x:x.split(",")[0])
     rdd2 = rdd1.map(lambda x: (x,1))
     rdd3 = rdd2.reduceByKey(lambda a,b: a+b)
     rdd4 = rdd3.map(lambda x:(x[1],x[0]))
     rdd5 = rdd4.sortByKey(False).take(5)
     print(rdd5)
def top5address(spark,file_path):
     rdd = spark.sparkContext.textFile(file_path)
     rdd1 = rdd.map(lambda x:x.split(",")[1])
     rdd2 = rdd1.map(lambda x: (x,1))
     rdd3 = rdd2.reduceByKey(lambda a,b: a+b)
     rdd4 = rdd3.map(lambda x:(x[1],x[0]))
     rdd5 = rdd4.sortByKey(False).take(5)
     print(rdd5)
def top5district(spark,file_path):
     rdd = spark.sparkContext.textFile(file_path)
     rdd1 = rdd.map(lambda x:x.split(",")[0])
     rdd2 = rdd1.map(lambda x: (x,1))
     rdd3 = rdd2.reduceByKey(lambda a,b: a+b)
     rdd4 = rdd3.map(lambda x:(x[1],x[0]))
     rdd5 = rdd4.sortByKey(False).take(5)
     print(rdd5)
# Driver class, creating the spark session and calling above methods..... 1app =1 driver. but multiple methods can exist
if __name__ == '__main__':
    spark:SparkSession = SparkSession.builder.master("local[1]").appName("bootcamp.com").getOrCreate()
    #top5category
    top5category(spark,"file:///home/takeo/pycharmprojects/DE43_Puskal/data1/jaddresscategory.csv")
    #top5address
    top5address(spark,"file:///home/takeo/pycharmprojects/DE43_Puskal/data1/jaddresscategory.csv")
    #top5district
    top5district(spark,"file:///home/takeo/pycharmprojects/DE43_Puskal/data1/jaddressdistrict.csv")




    # element_count(spark)
    # word_count(spark,"file:///home/takeo/test.txt")
    # word_count(spark,"file:///home/takeo/abc.txt")


# def element_count(spark):
#     data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
#     rdd = spark.sparkContext.parallelize(data)
#     print(rdd.count())
#
# def word_count(spark,file_path):
#     rdd = spark.sparkContext.textFile(file_path)
#     rdd2 = rdd.flatMap(lambda x: x.split(" "))
#     rdd3 = rdd2.map(lambda x: (x[0],1))
#     rdd5 = rdd3.reduceByKey(lambda a,b: a+b)
#     print(rdd5.collect())

# 
