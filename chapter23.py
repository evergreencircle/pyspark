#%%

#%%
from pyspark.sql import SparkSession
from time import sleep
from pyspark.sql.functions import expr, window, col

spark = SparkSession.builder.master("local").appName("Word Count")\
    .config("spark.some.config.option", "some-value")\
    .getOrCreate()
#%%

#%%
static = spark.read.json("/data/activity-data")
streaming = spark\
  .readStream\
  .schema(static.schema)\
  .option("maxFilesPerTrigger", 10)\
  .json("/data/activity-data")\
  .groupBy("gt")\
  .count()
query = streaming\
  .writeStream\
  .outputMode("complete")\
  .option("checkpointLocation", "/some/python/location/")\
  .queryName("test_python_stream")\
  .format("memory")\
  .start()
#%%
