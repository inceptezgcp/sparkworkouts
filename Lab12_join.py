from pyspark import SparkContext

def main():
    sc = SparkContext(master="local", appName="Lab12-Join")
    sc.setLogLevel("ERROR")
    
    #1,Lokesh
    srdd = sc.textFile("file:/home/hduser/students.csv")
    
    #1~90~80~95
    mrdd = sc.textFile("file:/home/hduser/marks.csv")
    
    #["1","Lokesh"]
    srdd1 = srdd.map(lambda x: x.split(","))
    
    #("1","Lokesh")
    srdd2 = srdd1.map(lambda x: (x[0],x[1]))
    
    #["1","90","80","95"]
    mrdd1 = mrdd.map(lambda x: x.split("~"))
    
    #("1",("90","80","95"))
    mrdd2 = mrdd1.map(lambda x : (x[0],(int(x[1]),int(x[2]),int(x[3]))))
    
    #("1",("Lokesh",("90","80","95")))
    joinrdd = srdd2.join(mrdd2)
    
    joinrdd.foreach(lambda x: print(x))
    
    studentdata = joinrdd.map(lambda x : (x[0],x[1][0],x[1][1][0],x[1][1][1],x[1][1][2]))
    
    print("=========StudentInfo====================")
    
    studentdata.foreach(lambda x: print(x))
    
    studenttotal = studentdata.map(lambda x : (x[0],x[1],x[2],x[3],x[4],x[2] + x[3] + x[4]))
    
    print("=========StudentInfo with totalmarks====================")
    
    studenttotal.foreach(lambda x: print(x))
    
    print("=========StudentInfo with Lowest Mark====================")
    
    
    sorteddata = studenttotal.sortBy(lambda x : x[5],True,1)
    
    lowestmark = sorteddata.first()
    
    print(lowestmark)
  
    print("=========StudentInfo with Highest Mark====================")
  
    highestmark = studenttotal.sortBy(lambda x : x[5],False,1).first()

    print(highestmark)





if __name__ == "__main__":
    main()
    