import pysam
from pyspark import SparkContext

num_of_unmapped = 0
num_of_kmers = 0
tabKmers=""
subs = []
K = 10

# create spark context
sc = SparkContext(appName="SAM_TOOLS")
FILE_NAME = "HG00096.chrom20.ILLUMINA.bwa.GBR.low_coverage.20120522.bam"
#bamUrl = "http://130.238.29.253:8080/swift/v1/1000-genomes-dataset/HG00096.chrom20.ILLUMINA.bwa.GBR.low_coverage.20120522.bam"
with pysam.AlignmentFile(FILE_NAME,"rb") as samfile:
	#lines = sc.textFile(FILE_NAME)
        for r in samfile.fetch(until_eof=True): ## fetch entire file, iterate over records
                if r.is_unmapped and not(r.mate_is_unmapped): 
                        num_of_unmapped +=1
			start_pos = r.query_alignment_start
			end_pos = r.query_alignment_end

                        for i in range(start_pos, end_pos - (K-1)): # why -9???????


                             
                                subs.append((r.query_sequence[i:i+K], r.reference_start+i))
				num_of_kmers+=1
                        

	for x in subs:
		print x


        print "Number of unmapped sequences: ", num_of_unmapped
        print "Number of generated K-mers (k=10): ", num_of_kmers
	print "range : " + str(r.query_alignment_end - r.query_alignment_start)

sc.stop()

