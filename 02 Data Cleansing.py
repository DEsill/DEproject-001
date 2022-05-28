from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()

dt = spark.read.csv('ws2_data.csv', header = True, inferSchema = True, )

spark