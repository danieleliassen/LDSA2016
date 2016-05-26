from __future__ import print_function
from pyspark import SparkContext, SparkConf
from operator import add
import swiftclient.client
import sys
import pysam

config = {
'user':'dael1787',
'key':'##',
'tenant_name':'c2016015',
'authurl': 'http://130.238.29.253:5000/v3'
}







def process(filename):
    num_of_unmapped = 0
    num_of_kmers = 0
    tabKmers=""
    list_of_kmers = []
    list_of_unmapped_starts = []
    K = 10
    path = "http://130.238.29.253:8080/swift/v1/1000-genomes-dataset/"+filename
    with pysam.AlignmentFile(path,"rb") as samfile:
        for r in samfile.fetch(until_eof=True):
            if r.is_unmapped and not(r.mate_is_unmapped):
                list_of_unmapped_starts.append((r.reference_start, 1))
                start_pos = r.query_alignment_start
                end_pos = r.query_alignment_end
                for i in range(start_pos, end_pos - (K-1)):
                    sequence = r.query_sequence[i:i+K]
                    if 'N' not in sequence:
                        list_of_kmers.append(((sequence, r.reference_start), 1))
    return (list_of_kmers, list_of_unmapped_starts)

def main():
    configuration = SparkConf().setAppName("1000-genomes Project")
    spark_context = SparkContext(conf=configuration)

    # Initializing variables
    container_name = "1000-genomes-dataset"

    conn = swiftclient.client.Connection(auth_version=3, **config)
    (storage_url, auth_token) = conn.get_auth()
    (response, content) = swiftclient.client.get_container(url=storage_url,container=container_name, token=auth_token)
    names = filter(lambda name: name[-4:-1] == 'bam', [c['name']+'\n' for c in content[:10]])
    results = spark_context.parallelize(names).map(process).collect()

    sys.stdout.write("{}".format(len(results)))
    return

if __name__ == '__main__':
    main()
