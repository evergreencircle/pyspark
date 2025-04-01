#%%
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructField, StructType, StringType, LongType
from pyspark.sql.functions import col, column, expr, lit, desc, asc, count, countDistinct, approx_count_distinct,\
    first, last, min, max, sum, sumDistinct, sum, count, avg, expr, sum, count, avg, expr,\
    var_pop, stddev_pop, var_samp, stddev_samp, skewness, kurtosis, corr, covar_pop, covar_samp,\
    collect_set, collect_list, to_date, dense_rank, rank, grouping_id
from pyspark.sql.window import Window




#%%
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
#%%
df = spark.read.format("csv")\
  .option("header", "true")\
  .option("inferSchema", "true")\
  .load("./data/retail-data/all/*.csv")\
  .coalesce(5)
df.cache()
df.createOrReplaceTempView("dfTable")
#%%
df.count() == 541909
#%%
df.select(count("StockCode")).show() # 541909

#%%
df.select(countDistinct("StockCode")).show() # 4070

#%%
df.select(approx_count_distinct("StockCode", 0.1)).show() # 3364

#%%
df.select(first("StockCode"), last("StockCode")).show()

#%%
df.select(min("Quantity"), max("Quantity")).show()

#%%
df.select(sum("Quantity")).show() # 5176450

#%%
df.select(sumDistinct("Quantity")).show() # 29310

#%%
df.select(
    count("Quantity").alias("total_transactions"),
    sum("Quantity").alias("total_purchases"),
    avg("Quantity").alias("avg_purchases"),
    expr("mean(Quantity)").alias("mean_purchases"))\
  .selectExpr(
    "total_purchases/total_transactions",
    "avg_purchases",
    "mean_purchases").show()
#%%
df.select(var_pop("Quantity"), var_samp("Quantity"),
  stddev_pop("Quantity"), stddev_samp("Quantity")).show()
#%%
df.select(skewness("Quantity"), kurtosis("Quantity")).show()

#%%
df.select(corr("InvoiceNo", "Quantity"), covar_samp("InvoiceNo", "Quantity"),
    covar_pop("InvoiceNo", "Quantity")).show()
#%%
df.agg(collect_set("Country"), collect_list("Country")).show()

#%%
df.groupBy("InvoiceNo", "CustomerId").count().show()
#%%
df.groupBy("InvoiceNo").agg(
    count("Quantity").alias("quan"),
    expr("count(Quantity)")).show()
#%%
df.groupBy("InvoiceNo").agg(expr("avg(Quantity)"),expr("stddev_pop(Quantity)"))\
  .show()
#%%
dfWithDate = df.withColumn("date", to_date(col("InvoiceDate"), "M/d/yyyy H:mm"))
dfWithDate.createOrReplaceTempView("dfWithDate")
#%%
dfWithDate.first()
#%%
dfWithDate.show()
#%%
windowSpec = Window\
  .partitionBy("CustomerId", "date")\
  .orderBy(desc("Quantity"))\
  .rowsBetween(Window.unboundedPreceding, Window.currentRow)
#%%
maxPurchaseQuantity = max(col("Quantity")).over(windowSpec)

#%%
purchaseDenseRank = dense_rank().over(windowSpec)
purchaseRank = rank().over(windowSpec)
#%%
dfWithDate.where("CustomerId IS NOT NULL").orderBy("CustomerId")\
  .select(
    col("CustomerId"),
    col("date"),
    col("Quantity"),
    purchaseRank.alias("quantityRank"),
    purchaseDenseRank.alias("quantityDenseRank"),
    maxPurchaseQuantity.alias("maxPurchaseQuantity")).show(50)
#%%

#%%
dfNoNull = dfWithDate.dropna()
dfNoNull.createOrReplaceTempView("dfNoNull")
#%%
dfNoNull.show()
#%%

rolledUpDF = dfNoNull.rollup("Date", "Country").agg(sum("Quantity"))\
  .selectExpr("Date", "Country", "`sum(Quantity)` as total_quantity")\
  .orderBy("Date", "Country")
rolledUpDF.show()
#%%
rolledUpDF.where("Country IS NULL").show()
rolledUpDF.where("Date IS NULL").show()
#%%
dfNoNull.cube("Date", "Country").agg(sum(col("Quantity")))\
  .select("Date", "Country", "sum(Quantity)").orderBy("Date").show(1000)

#%%
dfNoNull.cube("customerId","stockCode").agg(grouping_id(), sum("Quantity")).orderBy(desc("grouping_id()")).show()

#%%
pivoted = dfWithDate.groupBy("date").pivot("Country").sum()

#%%
pivoted.where("date > '2011-12-05'").select("date","`USA_sum(UnitPrice)`").show()
#%%
