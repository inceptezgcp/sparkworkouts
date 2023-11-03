from pyspark import SparkContext

def main():
    sc = SparkContext(master="local", appName="Lab14-accumulator")
    sc.setLogLevel("ERROR")
    
    rdd = sc.textFile("file:/usr/local/hadoop/logs/hadoop-hduser-namenode-localhost.localdomain.log",3)
    
    infocnt = sc.accumulator(0)
    
    warncnt = sc.accumulator(0)
    
    errorcnt = sc.accumulator(0)
    
    def updatecnt(x):
        if "INFO" in x:
            infocnt.add(1)
        elif "WARN" in x:
            warncnt.add(1)
        elif "ERROR" in x:
            errorcnt.add(1)
            
    rdd.foreach(updatecnt)
    
    print("Info count:" , str(infocnt.value))
    print("Warn count:" , str(warncnt.value))
    print("Error count:" , str(errorcnt.value))
    
main()