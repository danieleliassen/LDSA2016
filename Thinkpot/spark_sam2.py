import glob
import pysam
from pyspark import SparkContext

tabKmers=""
K = 10

sc = SparkContext(appName="SAM_TOOLS")

# create spark context
inputs = glob.glob('/home/ubuntu/LDSA2016/input/*.bam')
filename = sc.parallelize(inputs)


def getfile(path):
	subs = []
	bamfile =  pysam.AlignmentFile(path,"rb")
        for r in bamfile.fetch(until_eof=True): ## fetch entire file, iterate over records
                if r.is_unmapped and not(r.mate_is_unmapped):
                        start_pos = r.query_alignment_start
                        end_pos = r.query_alignment_end

                        for i in range(start_pos, end_pos - (K-1)): # why -9???????
                                subs.append((r.query_sequence[i:i+K], r.reference_start+i))
	return subs


filename.map(getfile).saveAsTextFile("/home/ubuntu/output")

sc.stop()

