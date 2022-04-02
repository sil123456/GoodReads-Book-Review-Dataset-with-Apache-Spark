# set up SparkContext for WordCount application
from pyspark import SparkContext
sc = SparkContext("local", "WordCount")

# the main map-reduce task
lines = sc.textFile("input.txt")
words = lines.flatMap(lambda line: line.split(" "))
word1s = words.map(lambda word: (word, 1))
wordCounts = word1s.reduceByKey(lambda a, b: a+b)
wordCounts.saveAsTextFile("output")
