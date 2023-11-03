from pyspark import SparkContext
sc = SparkContext(master="local", appName="Lab04-wholetext_ex")
sc.setLogLevel("ERROR")

rdd = sc.wholeTextFiles("file:/home/hduser/cust1,file:/home/hduser/cust2")

rdd.foreach(lambda x: print(x))

print("==============================")

def getcustdata(x):
    strtmp = ""
    if "cust1" in x[0]:
        tmp = x[1].split("\n")
        
        for l in tmp:
            print("test")
            print(l)
            #tmp1 = l.split(",")
            #strtmp =  strtmp + "\n" + tmp1[0] + "," + tmp1[2] + "," + tmp1[1] + "," + tmp1[3]
        return strtmp
    else:
        return x[1]

rdd1 = rdd.map(getcustdata)

rdd1.foreach(lambda x: print(x))

