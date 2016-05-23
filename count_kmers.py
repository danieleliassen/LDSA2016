#from pyspark import SparkContext, SparkConf
from operator import add
import sys

def main(sc):
    rdd1 = sc.textFile('/home/ubuntu/LDSA2016/kmers/')
    results = rdd1.map(lambda r: (r.split(',')[0], 1)).reduceByKey(add)
    #for (word, count) in results:
    #    sys.stdout.write('%s\t%s\n' % (word, count))
    return results

if __name__ == '__main__':
    main(sys.argv[1])
