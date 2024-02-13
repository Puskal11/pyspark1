from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField
from pyspark.sql.types import StringType, IntegerType

if __name__ == '__main__':
    spark:SparkSession = SparkSession.builder.master("local[1]").appName("bootcamp.com").getOrCreate()

    data= [(("Ford","Torino",""),140,3449,"US"),(("Chevrolet","Monte","Carlo"),150,3761,"US"),(("BMW","","2002"),113,2234,"Europe")]
    schema=StructType([
        StructField('carr',StructType([
            StructField('brand',StringType(),True),
            StructField('model',StringType(),True),
            StructField('model1',StringType(),True)
        ]),True),
        StructField('horsepower',IntegerType(),True),
        StructField("weight",IntegerType(),True),
        StructField('origin',StringType(),True)
    ])

    df = spark.createDataFrame(data = data, schema = schema)
    print(df.printSchema())
    print(df.show(truncate=False))


