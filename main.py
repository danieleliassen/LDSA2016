#from __future__ import print_function
from pyspark import SparkContext, SparkConf
from operator import add
import swiftclient.client
import sys
import pysam
import urllib
import config
import os
import time







def process(filename):
    num_of_unmapped = 0
    num_of_kmers = 0
    tabKmers=""
    result_list = []
    K = 10
    path = "http://130.238.29.253:8080/swift/v1/1000-genomes-dataset/"+filename
    local_path = "/home/ubuntu/"+filename  

    start_time_download = time.time()  
    for i in range(0,10):
    	try: 
            urllib.urlretrieve(path,local_path)
	    break
        except:
	    print "Failed to Download"

    start_time_mapping = time.time()  
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

    end_time = time.time()
    result_list.append(('TIME-DOWNLOAD', end_time - start_time_download))
    result_list.append(('TIME-MAPPING', end_time - start_time_mapping))

    os.remove(local_path)
    return result_list


def main():
    configuration = SparkConf().setAppName("1000-genomes Project")
    spark_context = SparkContext(conf=configuration)

    
    # Initializing variables
    container_name = "1000-genomes-dataset"

    conn = swiftclient.client.Connection(auth_version=3, **config.main())
    (storage_url, auth_token) = conn.get_auth()
    (response, content) = swiftclient.client.get_container(url=storage_url,container=container_name, token=auth_token)

    names = filter(lambda name: name[-4:-1] == 'bam', [c['name']+'\n' for c in content[:2]])

    filenames = spark_context.parallelize(names)
    mapped_data = filenames.flatMap(process)#.groupByKey()

    start_time_filtering = time.time()
    kmers = mapped_data.filter(lambda (k, (v, e)): k == "KMER").map(lambda (k, v): v).reduceByKey(add)
    positions = mapped_data.filter(lambda (k,v): k == "POSITION").map(lambda (k, v): v).reduceByKey(add)   

    time_filtering = time.time() - start_time_filtering
    time_mapping = mapped_data.filter(lambda (k,v): k == "TIME-MAPPING").reduceByKey(add).collect()
    time_download = mapped_data.filter(lambda (k,v): k == "TIME-DOWNLOAD").reduceByKey(add).collect()

    timing_file = open("timing.txt", "w")
    timing_file.write("Mapping " + time_mapping)
    timing_file.write("Mapping+Downloading " + time_download)
    timing_file.write("Filtering " + time_filtering)
    timing_file.close()

    kmer_file = open("kmers.txt", "w")
    for item in kmers.collect():
        print>>kmer_file, item
    kmer_file.close()


    pos_file = open("positions.txt", "w")
    for item in positions.collect():
        print>>pos_file, item
    pos_file.close()

    return
if __name__ == '__main__':
    main()
