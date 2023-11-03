from pyspark import SparkContext


def main():
    sc = SparkContext(master="local", appName="Lab07-header")
    sc.setLogLevel("ERROR")
    
    lst3 = ["Hadoop-Spark","Scala-Java","Hadoop-Java","Spark-Scala"]
    
    rdd = sc.parallelize(lst3)
    
    #rdd1(Array("Hadoop","Spark"),Array("Scala","Java"),Array("Spark","Scala"))
    
    rdd1 = rdd.map(lambda x: x.split("-"))
    
    #rdd2("Hadoop","Spark","Scala","Java","Spark","Scala")
    
    rdd2 = rdd1.flatMap(lambda x: x)
        
    print("=====map and flatmap=======")
    
    rdd2.foreach(lambda x: print(x))
   
    print("=====only flatmap=======")
    
    #rdd("Hadoop-Spark","Scala-Java","Hadoop-Java","Spark-Scala")
    #rdd3("Hadoop","Spark","Scala","Java","Spark","Scala")
    rdd3 = rdd.flatMap(lambda x : x.split("-"))
    
    rdd3.foreach(lambda x: print(x))
    
if __name__ == "__main__":
    main()
    
    