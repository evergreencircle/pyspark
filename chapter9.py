#%%
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import expr
#%%
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
#%%
csvFile = spark.read.format("csv")\
  .option("header", "true")\
  .option("mode", "FAILFAST")\
  .option("inferSchema", "true")\
  .load("./data/flight-data/csv/2010-summary.csv")
#%%
csvFile.show()
#%%
csvFile.write.format("csv").mode("overwrite").option("sep", "\t")\
  .save("./tmp/my-tsv-file.tsv")
#%%

spark.read.format("json").option("mode", "FAILFAST")\
  .option("inferSchema", "true")\
  .load("./data/flight-data/json/2010-summary.json").show(5)
#%%
csvFile.write.format("json").mode("overwrite").save("./tmp/my-json-file.json")

#%%

spark.read.format("parquet")\
  .load("./data/flight-data/parquet/2010-summary.parquet").show(5)
#%%
csvFile.write.format("parquet").mode("overwrite")\
  .save("./tmp/my-parquet-file.parquet")
#%%
spark.read.format("orc").load("./data/flight-data/orc/2010-summary.orc").show(5)

#%%
csvFile.write.format("orc").mode("overwrite").save("./tmp/my-json-file.orc")

#%%
csvFile.select("DEST_COUNTRY_NAME").write.text("./tmp/simple-text-file.txt")
#%%
csvFile.limit(10).select("DEST_COUNTRY_NAME", "count")\
.write.partitionBy("count").text("./tmp/five-csv-files2py.csv")
#%%
csvFile.limit(10).write.mode("overwrite").partitionBy("DEST_COUNTRY_NAME")\
.save("./tmp/partitioned-files.parquet")
#%%
