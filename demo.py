from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import functions
import os

os.environ[ "PYSPARK_PYTHON"] = "E:/SparkHadoop/Python/Python37/python.exe"

sc = SparkContext("local[*]", "sparkrdd")
rdd = sc.parallelize([1, 2, 3, 2, 1, 4, 5])
distinct_rdd = rdd.distinct()
for item in distinct_rdd.collect():
    print(item)