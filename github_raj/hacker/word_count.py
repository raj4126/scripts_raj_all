from pyspark import SparkContext, SparkConf
sc = SparkContext("local","PySpark Word Count Exmaple")

lines = spark.sparkContext.textFile("s3://bucketraj4126/input.txt")
lines.collect()
words = lines.flatMap(lambda line: line.split(" "))
words.collect()
wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b)
wordCounts.collect()
wordCounts.saveAsTextFile("s3:///bucketraj4126/output4.txt")

wordCounts.rdd.repartition(1).saveAsTextFile("s3:///bucketraj4126/output4.txt")
sc.parallelize([1,2,3]).saveAsTextFile("s3:///bucketraj4126/temp.txt")
#---------------------------------------------------------------------------

#aws s3 cp input.txt   s3://bucketraj4126/input.txt

#   spark-submit wc.py
