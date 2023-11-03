from pyspark import SparkContext
from pyspark.sql import SQLContext,Row


def main():
    sc = SparkContext(master="local", appName="Lab15")
    sc.setLogLevel("ERROR") 
    sqlc = SQLContext(sc)
    
    dept = [("Finance",10),("Marketing",20),("Sales",30),("IT",40)]
    rdd = sc.parallelize(dept)
    
    rdd1 = rdd.map(lambda x: Row(deptname=x[0],deptid=x[1]))
    
    df =  rdd1.toDF()
    df.show()
   
main()