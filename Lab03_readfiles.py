from pyspark import SparkContext


def mainprg():
    sc = SparkContext(master="local", appName="Lab02-Fact")
    sc.setLogLevel("ERROR")
    
    rdd = sc.textFile("file:/home/hduser/test.txt")
    rdd.foreach(lambda x: print(x))
    
    #Read multiple files
    rdd1 = sc.textFile("file:/home/hduser/file1,file:/home/hduser/file2,file:/home/hduser/file3")
    rdd1.foreach(lambda x: print(x))
    
    print("=======================")
    
    #Read from folder
    rdd2 = sc.textFile("file:/home/hduser/transcust")
    rdd2.foreach(lambda x: print(x))
    
    #Read only textfile from folder
    rdd3 = sc.textFile("file:/home/hduser/transcust/*.txt")
    rdd3.foreach(lambda x: print(x))
    
    #Read from multiple folders
    rdd4 = sc.textFile("file:/home/hduser/transcust,file:/home/hduser/transcust1")
    rdd4.foreach(lambda x: print(x))
    
    #Read from hdfs file
    rdd5 = sc.textFile("hdfs://localhost:54310/user/hduser/custs")
    rdd5.foreach(lambda x: print(x))
    
    #Read from local and hdfs file
    rdd6 = sc.textFile("file:/home/hduser/test.txt,hdfs://localhost:54310/user/hduser/custs")
    rdd6.foreach(lambda x: print(x))


if __name__ == "__main__":
    mainprg()

