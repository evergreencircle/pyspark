#%%
from pyspark.sql import SparkSession


spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
#%%
df = spark.range(500).toDF("number")
df = df.select(df["number"] + 10)
#%%
#df.show()
#%%
spark.range(2).collect()
#%%
