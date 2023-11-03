"""
---------------------------------
3. Data For Task
---------------------------------
("We", "Are" ,"Learning" , "Hadoop" , "From" , "Inceptez" , "We", "Are" ,"Learning" , "Spark" , "From" , "Inceptez.com" , "hadoop" , "HADOOP")

Accomplish the followings:-

1. Create an RDD using using the given words
2. Once RDD is created count all the words
3. Now filter out all the words which does not have Hadoop keyword, however make sure it would count all the different cases(upper/lower) as well

"""
from pyspark import SparkContext

    
sc = SparkContext(master="local", appName="Lab06-Workouts")
sc.setLogLevel("ERROR")

lst = ["We", "Are" ,"Learning" , "Hadoop" , "From" , "Inceptez" , "We", "Are" ,"Learning" , "Spark" , "From" , "Inceptez.com" , "hadoop" , "HADOOP"]

rdd = sc.parallelize(lst)

rdd1 = rdd.map(lambda x: (x,1))

rdd1.foreach(lambda x: print(x))
