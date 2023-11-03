from pyspark import SparkContext

    
sc = SparkContext(master="local", appName="Lab04-wholetextfile")
sc.setLogLevel("ERROR")

rdd = sc.wholeTextFiles("file:/home/hduser/transcust,hdfs://localhost:54310/user/hduser/transcust")
rdd.foreach(lambda x: print(x))

print("============Get only the filenames=================")
rdd1 = rdd.map(lambda x : x[0])
rdd1.foreach(lambda x: print(x))


print("============Get only the content=================")
rdd2 = rdd.map(lambda x : x[1])
rdd2.foreach(lambda x: print(x))