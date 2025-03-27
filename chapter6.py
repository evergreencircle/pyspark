#%%
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructField, StructType, StringType, LongType,  IntegerType, DoubleType
from pyspark.sql.functions import col, column, expr, lit, desc, asc, instr, pow, round, bround, corr,\
    count, mean, stddev_pop, min, max, monotonically_increasing_id, initcap, lower, upper,\
    lit, ltrim, rtrim, rpad, lpad, trim, regexp_replace, translate, regexp_extract, locate,\
    current_date, current_timestamp, date_add, date_sub, datediff, months_between, to_date,\
    to_timestamp, coalesce, struct, split, size, array_contains, split, explode, create_map,\
    get_json_object, json_tuple, to_json, from_json, udf


spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
#%%

#%%
df = spark.read.format("csv")\
  .option("header", "true")\
  .option("inferSchema", "true")\
  .load("./data/retail-data/by-day/2010-12-01.csv")
df.printSchema()
df.createOrReplaceTempView("dfTable")
#%%
df.select(lit(5), lit("five"), lit(5.0))

#%%
df.where(col("InvoiceNo") != 536365)\
  .select("InvoiceNo", "Description")\
  .show(5, False)
#%%
df.where("InvoiceNo <> 536365").show(5, False)
#%%
priceFilter = col("UnitPrice") > 600
descripFilter = instr(df.Description, "POSTAGE") >= 1
df.where(df.StockCode.isin("DOT")).where(priceFilter | descripFilter).show()

#%%
DOTCodeFilter = col("StockCode") == "DOT"
priceFilter = col("UnitPrice") > 600
descripFilter = instr(col("Description"), "POSTAGE") >= 1
df.withColumn("isExpensive", DOTCodeFilter & (priceFilter | descripFilter))\
  .where("isExpensive")\
  .select("unitPrice", "isExpensive").show(5)
#%%
df.withColumn("isExpensive", expr("NOT UnitPrice <= 250"))\
  .where("isExpensive")\
  .select("Description", "UnitPrice").show(5)
#%%
df.where(col("Description").eqNullSafe("hello")).show()
#%%
fabricatedQuantity = pow(col("Quantity") * col("UnitPrice"), 2) + 5
df.select(expr("CustomerId"), fabricatedQuantity.alias("realQuantity")).show(2)
#%%
df.selectExpr(
  "CustomerId",
  "(POWER((Quantity * UnitPrice), 2.0) + 5) as realQuantity").show(2)
#%%
df.select(round(lit("2.5")), bround(lit("2.5"))).show(2)

#%%
df.stat.corr("Quantity", "UnitPrice")
df.select(corr("Quantity", "UnitPrice")).show()
#%%
df.describe().show()

#%%
colName = "UnitPrice"
quantileProbs = [0.5]
relError = 0.05
df.stat.approxQuantile("UnitPrice", quantileProbs, relError) # 2.51
#%%
df.stat.crosstab("StockCode", "Quantity").show(5)

#%%
df.stat.freqItems(["StockCode", "Quantity"]).show(5)

#%%
df.select(monotonically_increasing_id()).show(2)

#%%
df.select(initcap(col("Description"))).show()

#%%
df.select(col("Description"),
    lower(col("Description")),
    upper(lower(col("Description")))).show(2)
#%%
df.select(
    ltrim(lit("    HELLO    ")).alias("ltrim"),
    rtrim(lit("    HELLO    ")).alias("rtrim"),
    trim(lit("    HELLO    ")).alias("trim"),
    lpad(lit("HELLO"), 3, " ").alias("lp"),
    rpad(lit("HELLO"), 10, " ").alias("rp")).show(2)
#%%
regex_string = "BLACK|WHITE|RED|GREEN|BLUE"
df.select(
  regexp_replace(col("Description"), regex_string, "COLOR").alias("color_clean"),
  col("Description")).show(2)
#%%
df.select(translate(col("Description"), "LEET", "1337"),col("Description"))\
  .show(2)
#%%
extract_str = "(BLACK|WHITE|RED|GREEN|BLUE)"
df.select(
     regexp_extract(col("Description"), extract_str, 1).alias("color_clean"),
     col("Description")).show(2)
#%%
containsBlack = instr(col("Description"), "BLACK") >= 1
containsWhite = instr(col("Description"), "WHITE") >= 1
df.withColumn("hasSimpleColor", containsBlack | containsWhite)\
  .where("hasSimpleColor")\
  .select("Description").show(3, False)
#%%
simpleColors = ["black", "white", "red", "green", "blue"]
def color_locator(column, color_string):
  return locate(color_string.upper(), column)\
          .cast("boolean")\
          .alias("is_" + color_string)
selectedColumns = [color_locator(df.Description, c) for c in simpleColors]
selectedColumns.append(expr("*")) # has to a be Column type
#%%
selectedColumns
#%%
df.select(*selectedColumns).where(expr("is_white OR is_red"))\
  .select("Description").show(3, False)
#%%
dateDF = spark.range(10)\
  .withColumn("today", current_date())\
  .withColumn("now", current_timestamp())
dateDF.createOrReplaceTempView("dateTable")
#%%
dateDF.printSchema()
#%%
dateDF.show()
#%%
dateDF.select(date_sub(col("today"), 5), date_add(col("today"), 5)).show(1)

#%%
dateDF.withColumn("week_ago", date_sub(col("today"), 7))\
  .select(datediff(col("week_ago"), col("today"))).show(1)
#%%
dateDF.select(
    to_date(lit("2016-01-01")).alias("start"),
    to_date(lit("2017-05-22")).alias("end"))\
  .select(months_between(col("start"), col("end"))).show(1)
#%%
spark.range(5).withColumn("date", lit("2017-01-01"))\
  .select(to_date(col("date"))).show(1)
#%%
dateFormat = "yyyy-dd-MM"
cleanDateDF = spark.range(1).select(
    to_date(lit("2017-12-11"), dateFormat).alias("date"),
    to_date(lit("2017-20-12"), dateFormat).alias("date2"))
cleanDateDF.createOrReplaceTempView("dateTable2")
#%%
cleanDateDF.show()
#%%
cleanDateDF.select(to_timestamp(col("date"), dateFormat)).show()

#%%
cleanDateDF.filter(col("date2") > lit("2017-12-12")).show()
#%%
cleanDateDF.filter(col("date2") > lit("2017-12-12")).show()
#%%
df.select(coalesce(col("Description"), col("CustomerId"))).show()

#%%
df.na.drop("all", subset=["StockCode", "InvoiceNo"])

#%%
df.na.fill("all", subset=["StockCode", "InvoiceNo"])

#%%
fill_cols_vals = {"StockCode": 5, "Description" : "No Value"}
df.na.fill(fill_cols_vals)
#%%
df.na.replace([""], ["UNKNOWN"], "Description")

#%%
complexDF = df.select(struct("Description", "InvoiceNo").alias("complex"))
complexDF.createOrReplaceTempView("complexDF")
#%%
complexDF.select("complex.Description")
complexDF.select(col("complex").getField("Description")).show(2)
#%%
df.selectExpr("(Description, InvoiceNo) as complex","*").show(2)
#%%
df.select(split(col("Description"), " ")).show(2)

#%%
df.select(split(col("Description"), " ").alias("array_col"))\
  .selectExpr("array_col[0]").show(2)
#%%
df.select(size(split(col("Description"), " "))).show(2) # shows 5 and 3

#%%
df.select(array_contains(split(col("Description"), " "), "WHITE")).show(2)

#%%
df.withColumn("splitted", split(col("Description"), " "))\
  .withColumn("exploded", explode(col("splitted")))\
  .select("Description", "InvoiceNo", "exploded").show(2)
#%%
df.select(create_map(col("Description"), col("InvoiceNo")).alias("complex_map"))\
  .show(2)
#%%
df.select(create_map(col("Description"), col("InvoiceNo")).alias("complex_map"))\
  .selectExpr("complex_map['WHITE METAL LANTERN']").show(2)
#%%
df.select(create_map(col("Description"), col("InvoiceNo")).alias("complex_map"))\
  .selectExpr("explode(complex_map)").show(2)
#%%
jsonDF = spark.range(1).selectExpr("""
  '{"myJSONKey" : {"myJSONValue" : [1, 2, 3]}}' as jsonString""")
#%%
jsonDF.show(1)
#%%
jsonDF.select(
    get_json_object(col("jsonString"), "$.myJSONKey.myJSONValue[1]").alias("column"),
    json_tuple(col("jsonString"), "myJSONKey")).show(2)
#%%
df.selectExpr("(InvoiceNo, Description) as myStruct")\
  .select(to_json(col("myStruct"))).show(1)
#%%
parseSchema = StructType((
  StructField("InvoiceNo",StringType(),True),
  StructField("Description",StringType(),True)))
df.selectExpr("(InvoiceNo, Description) as myStruct")\
  .select(to_json(col("myStruct")).alias("newJSON"))\
  .select(from_json(col("newJSON"), parseSchema).alias("newJSON1"), col("newJSON")).select(col("newJSON1")).show(1)
#%%
udfExampleDF = spark.range(5).toDF("num")
def power3(double_value):
  return double_value ** 3
power3(2.0)
#%%
power3udf = udf(power3)

#%%
udfExampleDF.select(power3udf(col("num"))).show(4)

#%%
udfExampleDF.selectExpr("power3(num)").show(2)

#%%
spark.udf.register("power3py", power3, IntegerType())

#%%
udfExampleDF.selectExpr("power3py(num)").show(2)
#%%
