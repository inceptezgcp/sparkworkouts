from pyspark import SparkContext
import pyspark



    
sc = SparkContext(master="local", appName="Lab07-header")
sc.setLogLevel("ERROR")

rdd = sc.textFile("file:/home/hduser/user.csv")
    
header = rdd.first()

rdd1 = rdd.filter(lambda x : x != header)

rdd1.foreach(lambda x: print(x))

print("===========================")

rdd2 = rdd1.filter(lambda x : "myself" not in x)
rdd2.persist(pyspark.StorageLevel.useMemory)

rdd2.cache()

rdd2.foreach(lambda x: print(x))


