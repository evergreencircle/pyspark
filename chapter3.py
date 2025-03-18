#%%
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.sql import Row

from pyspark.sql.functions import window, column, desc, col
from pyspark.sql.functions import date_format, col
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.evaluation import ClusteringEvaluator


#from pyspark.sql.functions import max, desc


spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
#%%
#! pip install numpy
#%%
staticDataFrame = spark.read.format("csv")\
  .option("header", "true")\
  .option("inferSchema", "true")\
  .load("./data/retail-data/by-day/*.csv")

staticDataFrame.createOrReplaceTempView("retail_data")
staticSchema = staticDataFrame.schema
#%%
staticSchema
#%%
staticDataFrame\
  .selectExpr(
    "CustomerId",
    "(UnitPrice * Quantity) as total_cost",
    "InvoiceDate")\
  .groupBy(
    col("CustomerId"), window(col("InvoiceDate"), "1 day"))\
  .sum("total_cost")\
  .sort(desc("sum(total_cost)"))\
  .show(5)
#%%
spark.conf.set("spark.sql.shuffle.partitions", "5")
#%%
streamingDataFrame = spark.readStream\
    .schema(staticSchema)\
    .option("maxFilesPerTrigger", 1)\
    .format("csv")\
    .option("header", "true")\
    .load("./data/retail-data/by-day/*.csv")
#%%
streamingDataFrame.isStreaming
#%%
purchaseByCustomerPerHour = streamingDataFrame\
  .selectExpr(
    "CustomerId",
    "(UnitPrice * Quantity) as total_cost",
    "InvoiceDate")\
  .groupBy(
    col("CustomerId"), window(col("InvoiceDate"), "1 day"))\
  .sum("total_cost")
#%%
purchaseByCustomerPerHour.writeStream\
    .format("memory")\
    .queryName("customer_purchases")\
    .outputMode("complete")\
    .start()
#%%
spark.sql("""
  SELECT *
  FROM customer_purchases
  ORDER BY `sum(total_cost)` DESC
  """)\
  .show(5)
#%%
staticDataFrame.printSchema()
#%%
preppedDataFrame = staticDataFrame\
  .na.fill(0)\
  .withColumn("day_of_week", date_format(col("InvoiceDate"), "EEEE"))\
  .coalesce(5)
#%%
preppedDataFrame.show(3)
#%%
trainDataFrame = preppedDataFrame\
  .where("InvoiceDate < '2011-07-01'")
testDataFrame = preppedDataFrame\
  .where("InvoiceDate >= '2011-07-01'")
#%%
trainDataFrame.count()
testDataFrame.count()
#%%
indexer = StringIndexer()\
  .setInputCol("day_of_week")\
  .setOutputCol("day_of_week_index")
#%%
encoder = OneHotEncoder()\
  .setInputCol("day_of_week_index")\
  .setOutputCol("day_of_week_encoded")
#%%
vectorAssembler = VectorAssembler()\
  .setInputCols(["UnitPrice", "Quantity", "day_of_week_encoded"])\
  .setOutputCol("features")
#%%
transformationPipeline = Pipeline()\
  .setStages([indexer, encoder, vectorAssembler])
#%%
fittedPipeline = transformationPipeline.fit(trainDataFrame)

#%%
transformedTraining = fittedPipeline.transform(trainDataFrame)

#%%
kmeans = KMeans()\
  .setK(20)\
  .setSeed(1)
#%%
transformedTraining.cache()
#%%
kmModel = kmeans.fit(transformedTraining)
#%%
transformedTest = fittedPipeline.transform(testDataFrame)

#%%
testDataFrame
#%%
transformedTest
#%%
spark.sparkContext.parallelize([Row(1), Row(2), Row(3)]).toDF()

#%%
