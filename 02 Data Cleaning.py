from pyspark.sql import SparkSession
import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd

spark = SparkSession.builder.master("local[*]").getOrCreate()
dt = spark.read.csv('ws2_data.csv', header = True, inferSchema = True, )

from pyspark.sql import functions as f

dt_clean = dt.withColumn("timestamp",
                        f.to_timestamp(dt.timestamp, 'yyyy-mm-dd HH:mm:ss')
                        )

dt.createOrReplaceTempView("data")
dt_clean = spark.sql("""
SELECT DISTINCT country
FROM data
ORDER BY country
""")

dt_clean = spark.sql("""
SELECT * 
FROM data
WHERE country = 'Japane'
""")

dt_clean = spark.sql("""
SELECT timestamp, user_id, book_id,
    CASE WHEN country = 'Japane' THEN 'Japan' ELSE country END AS country,
price
FROM data
""")

dt_clean = spark.sql("""
SELECT timestamp,
CASE WHEN user_id = 'ca86d17200' THEN 'ca86d172' ELSE user_id END AS user_id,
book_id, country, price
FROM data
""")

dt_clean = spark.sql("""
SELECT timestamp,
CASE WHEN user_id is NULL THEN '000000' ELSE user_id END AS user_id,
book_id, country, price
FROM data
""")

dt_pd = dt.toPandas()
sns.boxplot(x = dt_pd['price'])
