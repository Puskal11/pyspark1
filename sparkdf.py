from pyspark.sql import SparkSession


# def writeDf(df, format):
#     if format== 'parquet':
#     df.write.parquet("file:///tmp/sparkoutput/pycharm_parquet")
#     elif format == 'orc':
#     df.write.orc("file:///tmp/sparkoutput/pycharm_parquet")
#     elif format == 'json':
#     df.write.json("file:///tmp/sparkoutput/pycharm_parquet")


def writeDf1(df, format,filepath):
    print("writing into",format)
    df.write.format(format).mode('overwrite').save(filepath)


if __name__ == '__main__':
    spark:SparkSession = SparkSession.builder.master("local[1]").appName("bootcamp.com").getOrCreate()

    df=spark.read.options(header='True',inferSchema='True').csv("file:///home/takeo/pycharmprojects/zipcodes.csv")

    writeDf1(df, "orc","file:///tmp/spark/zipcodes/orc")
    writeDf1(df, "csv","file:///tmp/spark/zipcodes/csv")
    writeDf1(df, "json","file:///tmp/spark/zipcodes/json")


