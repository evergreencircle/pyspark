#%%
from pyspark.sql import SparkSession
from time import sleep
from pyspark.sql.functions import expr

spark = SparkSession.builder.master("local").appName("Word Count")\
    .config("spark.some.config.option", "some-value")\
    .getOrCreate()
#%%
static = spark.read.json("./data/activity-data/")
dataSchema = static.schema
#%%
streaming = spark.readStream.schema(dataSchema).option("maxFilesPerTrigger", 1)\
  .json("./data/activity-data")
#%%
activityCounts = streaming.groupBy("gt").count()

#%%
spark.conf.set("spark.sql.shuffle.partitions", 5)
#%%
activityQuery = activityCounts.writeStream.queryName("activity_counts")\
  .format("memory").outputMode("complete")\
  .start()
#%%
activityQuery.awaitTermination()
#%%
spark.streams.active
#%%
for x in range(10):
    spark.sql("SELECT * FROM activity_counts").show()
    sleep(1)

#%%
simpleTransform = streaming.withColumn("stairs", expr("gt like '%stairs%'"))\
  .where("stairs")\
  .where("gt is not null")\
  .select("gt", "model", "arrival_time", "creation_time")\
  .writeStream\
  .queryName("simple_transform1")\
  .format("memory")\
  .outputMode("append")\
  .start()
#%%
for x in range(5):
    spark.sql("SELECT * FROM simple_transform1").show()
    sleep(1)
#%%
deviceModelStats = streaming.cube("gt", "model").avg()\
  .drop("avg(Arrival_time)")\
  .drop("avg(Creation_Time)")\
  .drop("avg(Index)")\
  .writeStream.queryName("device_counts").format("memory")\
  .outputMode("complete")\
  .start()
#%%
for x in range(10):
    spark.sql("SELECT * FROM device_counts").show()
    sleep(1)
#%%
historicalAgg = static.groupBy("gt", "model").avg()
deviceModelStats = streaming.drop("Arrival_Time", "Creation_Time", "Index")\
  .cube("gt", "model").avg()\
  .join(historicalAgg, ["gt", "model"])\
  .writeStream.queryName("device_counts").format("memory")\
  .outputMode("complete")\
  .start()
#%%
for x in range(2):
    spark.sql("SELECT * FROM device_counts").show()
    sleep(1)
#%%
# Subscribe to 1 topic
df1 = spark.readStream.format("kafka")\
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")\
  .option("subscribe", "topic1")\
  .load()
# Subscribe to multiple topics
df2 = spark.readStream.format("kafka")\
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")\
  .option("subscribe", "topic1,topic2")\
  .load()
# Subscribe to a pattern
df3 = spark.readStream.format("kafka")\
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")\
  .option("subscribePattern", "topic.*")\
  .load()
#%%
df1.selectExpr("topic", "CAST(key AS STRING)", "CAST(value AS STRING)")\
  .writeStream\
  .format("kafka")\
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")\
  .option("checkpointLocation", "/to/HDFS-compatible/dir")\
  .start()
df1.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")\
  .writeStream\
  .format("kafka")\
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")\
  .option("checkpointLocation", "/to/HDFS-compatible/dir")\
  .option("topic", "topic1")\
  .start()
#%%
socketDF = spark.readStream.format("socket")\
  .option("host", "localhost").option("port", 9999).load()
#%%

activityCounts.writeStream.trigger(processingTime='5 seconds')\
  .format("console").outputMode("complete").start()
#%%

activityCounts.writeStream.trigger(once=True)\
  .format("console").outputMode("complete").start()
#%%
