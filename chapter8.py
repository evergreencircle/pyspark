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
person = spark.createDataFrame([
    (0, "Bill Chambers", 0, [100]),
    (1, "Matei Zaharia", 1, [500, 250, 100]),
    (2, "Michael Armbrust", 1, [250, 100])])\
  .toDF("id", "name", "graduate_program", "spark_status")
graduateProgram = spark.createDataFrame([
    (0, "Masters", "School of Information", "UC Berkeley"),
    (2, "Masters", "EECS", "UC Berkeley"),
    (1, "Ph.D.", "EECS", "UC Berkeley")])\
  .toDF("id", "degree", "department", "school")
sparkStatus = spark.createDataFrame([
    (500, "Vice President"),
    (250, "PMC Member"),
    (100, "Contributor")])\
  .toDF("id", "status")
#%%
person.createOrReplaceTempView("person")
graduateProgram.createOrReplaceTempView("graduateProgram")
sparkStatus.createOrReplaceTempView("sparkStatus")
#%%
joinExpression = person["graduate_program"] == graduateProgram['id']
#%%
wrongJoinExpression = person["name"] == graduateProgram["school"]

#%%
person.join(graduateProgram, joinExpression).show()
#%%
joinType = "inner"

#%%
person.join(graduateProgram, joinExpression, joinType).show()

#%%
joinType = "outer"
#%%
person.join(graduateProgram, joinExpression, joinType).show()

#%%
joinType = "left_outer"

#%%
graduateProgram.join(person, joinExpression, joinType).show()
#%%
joinType = "right_outer"

#%%
person.join(graduateProgram, joinExpression, joinType).show()

#%%
joinType = "left_semi"

#%%
graduateProgram.join(person, joinExpression, joinType).show()

#%%
gradProgram2 = graduateProgram.union(spark.createDataFrame([
    (0, "Masters", "Duplicated Row", "Duplicated School")]))

gradProgram2.createOrReplaceTempView("gradProgram2")
#%%
gradProgram2.join(person, joinExpression, joinType).show()
#%%
joinType = "left_anti"
#%%
graduateProgram.join(person, joinExpression, joinType).show()

#%%
joinType = "cross"
#%%
graduateProgram.crossJoin(person).show(10)

#%%
person.withColumnRenamed("id", "personId")\
  .join(sparkStatus, expr("array_contains(spark_status, id)")).show()
#%%
gradProgramDupe = graduateProgram.withColumnRenamed("id", "graduate_program")
#%%
joinExpr = gradProgramDupe["graduate_program"] == person["graduate_program"]
#%%
person.join(gradProgramDupe, joinExpr).show()
#%%
person.join(gradProgramDupe, joinExpr).select("graduate_program").show()
#%%
person.join(gradProgramDupe,"graduate_program").select("graduate_program").show()
#%%
person.join(gradProgramDupe, joinExpr).drop(person.graduate_program).show()
#%%
gradProgram3 = graduateProgram.withColumnRenamed("id","grad_id")
#%%
joinExpr = person['graduate_program'] == gradProgram3['grad_id']
                                                             
#%%
person.join(gradProgram3, joinExpr).show()
#%%
