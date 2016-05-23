from operator import add
import pysam
import sys
from pyspark import SparkContext


fil = "/mnt/volume/PROCESSED_FILES/PROCESSED_HG03270.chrom20.ILLUMINA.bwa.ESN.low_coverage.20121211.bam"
sc = SparkContext(appName="SAM_TOOLS")

my_rdd = sc.textFile(fil)
csv_parsed = my_rdd.map(lambda x: x.split(",")) # split the comma-separated data
positions = csv_parsed.map(lambda x: (int(round(int(x[1]), -3)), 1)).reduceByKey(add) # extract all positions, round it to closest thousand-multiple, emit the count, reduce.
positions.saveAsTextFile("/home/ubuntu/OUTPUT") # save as file
sc.stop()

