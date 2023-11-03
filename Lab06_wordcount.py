from pyspark import SparkContext

    
sc = SparkContext(master="local", appName="Lab06-Wordcount")
sc.setLogLevel("ERROR")

#Stage-1
rdd = sc.parallelize(["spark,scala,hive","hive,java,spark","spark,python,scala"],2)
rdd1 = rdd.flatMap(lambda x: x.split(","))
rdd2 = rdd1.map(lambda x: (x,1))
    
#Stage-2
rdd3 = rdd2.reduceByKey(lambda a,b : a + b,1)
rdd3.foreach(lambda x:print(x))