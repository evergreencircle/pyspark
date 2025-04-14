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
spark.sparkContext
#%%
spark.range(10).rdd


#%%
spark.range(10).toDF("id").rdd.map(lambda row: row[0])
#%%
spark.range(10).rdd.toDF()

#%%
myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple"\
  .split(" ")
words = spark.sparkContext.parallelize(myCollection, 2)
#%%
words.setName("myWords")
words.name() # myWords
#%%
words.distinct().count()
#%%
def startsWithS(individual):
  return individual.startswith("S")
#%%
words.filter(lambda word: startsWithS(word)).collect()

#%%
words2 = words.map(lambda word: (word, word[0], word.startswith("S")))
words2.collect()

#%%
words2.filter(lambda record: record[2]).take(5)

#%%
words.flatMap(lambda word: list(word)).take(5)

#%%
words.sortBy(lambda word: len(word) * -1).take(2)

#%%
fiftyFiftySplit = words.randomSplit([0.5, 0.5])

#%%
fiftyFiftySplit
#%%
spark.sparkContext.parallelize(range(1, 21)).reduce(lambda x, y: x + y) # 210

#%%
def wordLengthReducer(leftWord, rightWord):
  if len(leftWord) > len(rightWord):
    return leftWord
  else:
    return rightWord

words.reduce(wordLengthReducer)
#%%
words.getStorageLevel()

#%%
words.mapPartitions(lambda part: [1]).sum() # 2

#%%
def indexedFunc(partitionIndex, withinPartIterator):
  return ["partition: {} => {}".format(partitionIndex,
    x) for x in withinPartIterator]
words.mapPartitionsWithIndex(indexedFunc).collect()
#%%
spark.sparkContext.parallelize(["Hello", "World"], 2).glom().collect()

#%%
