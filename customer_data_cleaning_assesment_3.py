from pyspark.sql import SparkSession

def removeDup(spark):
    df1 = df.dropDuplicates()
    print(df1.show(truncate=False))

def removeDupAgeHeight(spark):
    df1=df.dropDuplicates(["Age","Height"])
    print(df1.show(truncate=False))


if __name__ == '__main__':
    spark: SparkSession = SparkSession.builder.master("local[1]").appName("bootcamp.com").getOrCreate()
    data = [("Smith", 23, 5.3), ("Rashmi", 27, 5.8),("Smith", 23, 5.3), ("Payal", 27, 5.8), ("Megha", 27, 5.4)]
    columns = ["Name", "Age", "Height"]
    df = spark.createDataFrame(data=data, schema=columns)
    print(df.show(truncate=False))

    removeDup(spark)
    removeDupAgeHeight(spark)
