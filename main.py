from pyspark import SparkContext, SparkConf
from operator import add
import sys
import count_kmers

def main():
    configuration = SparkConf().setAppName("1000-genomes Project")
    spark_context = SparkContext(conf=configuration)
    for item in count_kmers.main(spark_context).collect():
        word, count = item
        sys.stdout.write("{0}, {1}\n".format(word, count))
    return

if __name__ == '__main__':
    main()
