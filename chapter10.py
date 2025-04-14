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
spark.read.json("./data/flight-data/json/2015-summary.json")\
  .createOrReplaceTempView("flights") # DF => SQL
#%%
spark.sql("""
SELECT DEST_COUNTRY_NAME, sum(count)
FROM some_sql_view GROUP BY DEST_COUNTRY_NAME
""")\
  .where("DEST_COUNTRY_NAME like 'S%'").where("`sum(count)` > 10")\
  .count() # SQL => DF

#%%
results = spark.sql(
  "CREATE TABLE fligh_csv1 (DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count LONG) USING csv OPTIONS (path './data/flight-data/csv/2015-summary.csv')")
#%%
spark.sql("select * from fligh_csv1").show(3)
#%%
spark.sql("SELECT * FROM flights f1 WHERE EXISTS (SELECT 1 FROM flights f2 WHERE f1.dest_country_name = f2.origin_country_name) AND EXISTS (SELECT 1 FROM flights f2 WHERE f2.dest_country_name = f1.origin_country_name)").show()

#%%
spark.sql("SELECT * FROM flights f1 WHERE EXISTS (SELECT 1 FROM flights f2 WHERE f1.dest_country_name = f2.origin_country_name)").show()
#%%
spark.sql("SELECT * FROM flights").show()
#%%
