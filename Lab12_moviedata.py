from pyspark import SparkContext

"""
---------------------------------
11.    Data For Task
---------------------------------

file:/home/hduser/movies.csv

Accomplish the followings:-

1. Load the movies data into the RDD. 
2. List the movies that having a rating greater than or equal to 4
3. List the movies that are released after 1980
4. List the movies by release year
5. Save the final data in HDFS in location "/sparkworkouts/movierelease" into 2 files

"""

def main():
    sc = SparkContext(master="local", appName="Lab12-Movieprocess")
    sc.setLogLevel("ERROR")
    
    rdd = sc.textFile("file:/home/hduser/movies.csv")
    
    rdd1 = rdd.map(lambda x: x.split(","))
    
    print("==========Movie by Rating=================")
    getmoviesbyrating(rdd1)
    
    print("==========Movie by Release Year=================")
    getmoviesbyyear(rdd1)
    
    print("==========Group Movies by Release Year=================")
    getmoviesgroupbyreleaseyear(rdd1)
  

def getmoviesbyrating(rdd):
    rdd2 = rdd.filter(lambda x: float(x[3]) >= 3.5)
    rdd2.foreach(lambda x: print(x))  
    
def getmoviesbyyear(rdd):
    rdd2 = rdd.filter(lambda x: int(x[2]) >= 1980)
    rdd2.foreach(lambda x: print(x))  
    
def getmoviesgroupbyreleaseyear(rdd):
    rdd1 = rdd.map(lambda x: (x[2],x[1]))
    rdd2 = rdd1.groupByKey()
    rdd3 = rdd2.map(lambda x: (x[0],list(x[1])))
    #rdd4 = rdd3.coalsece(1).sortByKey(True)
    rdd4 = rdd3.sortByKey(True,1)
    rdd4.foreach(lambda x: print(x))
    
    print("============Movies by count===============")
    rdd5 = rdd4.map(lambda x: (x[0],len(x[1])))
    rdd5.foreach(lambda x: print(x))
    

    
    
    
    
    
    
    
    
    
    
if __name__ == "__main__":
    main()
    

