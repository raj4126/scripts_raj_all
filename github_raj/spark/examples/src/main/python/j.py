from __future__ import print_function

import sys
from operator import add

from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("PythonWordCount").getOrCreate()
lines = spark.read.text('wc.txt').rdd.map(lambda r: r[0])
counts = lines.flatMap(lambda x: x.split(' ')).map(lambda x: (x, 1)).reduceByKey(add).sortByKey()
output = counts.collect()
for (word, count) in output:
    print("%s: %i" % (word, count))

print("HELLO")
spark.stop()
