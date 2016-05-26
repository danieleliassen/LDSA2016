import pysam
import sys
num_of_unmapped = 0
num_of_kmers = 0
tabKmers=""
subs = []
K = 10
FILE_NAME = "http://130.238.29.253:8080/swift/v1/1000-genomes-dataset/"+str(sys.argv[1])
with pysam.AlignmentFile(FILE_NAME,"rb") as samfile:
        for r in samfile.fetch(until_eof=True): ## fetch entire file, iterate over records
	      	if r.is_unmapped and not(r.mate_is_unmapped):
                	num_of_unmapped +=1
			start_pos = r.query_alignment_start
			end_pos = r.query_alignment_end
                        for i in range(start_pos, end_pos - (K-1)): # why -9???????
                        	subs.append(str(r.query_sequence[i:i+K])+", "+str(r.reference_start+i))
				num_of_kmers+=1

	for x in subs:
		print x

