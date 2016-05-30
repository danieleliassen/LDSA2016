#from __future__ import print_function
from pyspark import SparkContext, SparkConf
from operator import add
#import swiftclient.client
import sys
import pysam
import urllib
#import config
import os







def process(filename):
    num_of_unmapped = 0
    num_of_kmers = 0
    tabKmers=""
    result_list = []
    K = 10
    path = "http://130.238.29.253:8080/swift/v1/1000-genomes-dataset/"+filename
    local_path = "/home/ubuntu/"+filename    
    
    for i in range(0,10):
    	try: 
            urllib.urlretrieve(path,local_path)
	    break
        except:
	    print "Failed to Download"

    with pysam.AlignmentFile(local_path,"rb") as samfile:
        try:
            data = samfile.fetch(until_eof=True)
            for r in data:
                if r.is_unmapped and not(r.mate_is_unmapped):
                    result_list.append(('POSITION', (r.reference_start, 1)))
                    start_pos = r.query_alignment_start
                    end_pos = r.query_alignment_end
                    for i in range(start_pos, end_pos - (K-1)):
                        sequence = r.query_sequence[i:i+K]
                        if 'N' not in sequence:
                            result_list.append(('KMER', (sequence, 1)))
        except IOError as e:
            print e
            pass

    os.remove(local_path)
    return result_list
#("POSITION", (pos, 1)), ("KMER", (sequence, 1))
def main():
    configuration = SparkConf().setAppName("1000-genomes Project")
    spark_context = SparkContext(conf=configuration)

    
    # Initializing variables
    container_name = "1000-genomes-dataset"

 #   conn = swiftclient.client.Connection(auth_version=3, **config.main())
 #   (storage_url, auth_token) = conn.get_auth()
 #   (response, content) = swiftclient.client.get_container(url=storage_url,container=container_name, token=auth_token)

#    names = filter(lambda name: name[-4:-1] == 'bam', [c['name']+'\n' for c in content[:20]])

    names = ["HG00096.chrom20.ILLUMINA.bwa.GBR.low_coverage.20120522.bam","HG00097.chrom20.ILLUMINA.bwa.GBR.low_coverage.20130415.bam"]
    filenames = spark_context.parallelize(names)
    mapped_data = filenames.flatMap(process)#.groupByKey()
    kmers = mapped_data.filter(lambda (k, (v, e)): k == "KMER").map(lambda (k, v): v).reduceByKey(add)
    positions = mapped_data.filter(lambda (k,v): k == "POSITION").map(lambda (k, v): v).reduceByKey(add)

    # mm = positions.collect()
    # l=0
    # for i in mm:
    #     print i
    #     l = l+1
    #     if l == 1:
    #         brea:#k
    for element in positions.collect():#[:10]:
        print(element)
    for element in kmers.collect():#[:10]:
        print(element)
    return

if __name__ == '__main__':
    main()
