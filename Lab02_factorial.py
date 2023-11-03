from pyspark import SparkContext


def main():
    sc = SparkContext(master="local", appName="Lab02-Fact")
    lst = [5,3,7]
    rddlst = sc.parallelize(lst)
    factrdd = rddlst.map(findfactorial)
    factrdd.foreach(lambda x: print(x))

def findfactorial(a):
    fact = 1
    for i in range(2,a+1):
        fact = fact * i
    return fact        
        

if __name__ == "__main__":
    main()
