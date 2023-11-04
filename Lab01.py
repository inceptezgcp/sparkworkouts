from pyspark import SparkContext
from pyspark import SparkConf

print("Welcome to spark using python scripting")

sconf = SparkConf().setAppName("First-Spark").setMaster("local")

sc = SparkContext(conf=sconf)
lst = [10,20,30]
rdd = sc.parallelize(lst)
total = rdd.sum()

print("Total:{}".format(total))
print("Completed")
