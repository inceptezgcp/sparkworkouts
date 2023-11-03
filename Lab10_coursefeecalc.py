from pyspark import SparkContext



def main():
    sc = SparkContext(master="local", appName="Lab10-header")
    sc.setLogLevel("ERROR")
    
    
    rdd = sc.textFile("file:/home/hduser/coursefee.txt",2)
    
    rdd1 = rdd.map(lambda x: x.split(",")).coalesce(1)
    
    rdd2 = rdd1.map(lambda x: (x[0],int(x[1]),int(x[2]))).repartition(2)
    
    rdd3 = rdd2.coalesce(1)
    
    rdd4 = rdd3.map(lambda x : [x[0],(x[1] * x[2]) /  100,(x[1] * x[2]) /  100 + x[1], x[1],x[2]]).map(lambda x: [x[0],str(x[1]),str(x[2]),str(x[3]),str(x[4])])
    
    #rdd3.foreach(println)
    
    rdd5 = rdd4.map(lambda x : ",".join(x))
    
    headerstr = ["CourseName,Taxamount,Totalfee,actualfee,TaxPercent"]
    
    rddheader = sc.parallelize(headerstr)
    
    rdd6 = rddheader.union(rdd5)
    
    rdd6.coalesce(1).saveAsTextFile("file:/home/hduser/coursefeedetails")
    
    print("Data stored in file")
   
    
if __name__ == "__main__":
    main()
    
    