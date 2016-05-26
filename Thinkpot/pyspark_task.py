from pyspark import SparkContext, SparkConf
import pysam, os, sys
import swiftclient.client


conf = SparkConf().setAppName("K-mer count")#.setMaster(master)
sc = SparkContext(conf=conf)

K = 10

def download_container(config, container_name, samples):
    conn = swiftclient.client.Connection(auth_version=3, **config)
    (storage_url, auth_token) = conn.get_auth()
    (response, content) = swiftclient.client.get_container(url=storage_url,container=container_name, token=auth_token)
    bam_files = []
    for bucket in content:
        filename = bucket['name']
        if filename[-3:] == 'bam' and filename in samples:
                bam_files.append(download_bam_from_swift(conn, container_name, filename))
    return bam_files

def download_bam_from_swift(conn, container_name, filename):
    while True:
        try:
            _, file_contents = conn.get_object(container_name, filename)
        except Exception as pe:
            print pe,'\nDownload failed, retrying file: \n', filename
            continue
        break
    return file_contents

def count_kmers(bam):
    indices = range(bam.query_alignment_start, bam.query_alignment_end - (K-1))
    kmers = []
    for index in indices:
        kmers.append((bam.reference_start + index, bam.query_sequence[index:index+K]))
        # Mapping result, value separation by the tab-character.
        out = str(bam.reference_start + index) + '\t' + str(bam.query_sequence[index:index+K]) + '\t1\n'
        #sys.stdout.write(out)
    return (len(indices) - (K-1), kmers)

def parse_bam_file(bam):
    kmers = []
    unmapped = 0
    total_count = 0
    for sample in bam.fetch(until_eof=True):
            if sample.is_unmapped and not(sample.mate_is_unmapped):
                    count,kmers_in_sample = count_kmers(sample)
                    unmapped += 1
                    total_count += count
                    [kmers.append(kmer) for kmer in kmers_in_sample]
    return (total_count, kmers)

def get_bam_file(filename):
    return pysam.AlignmentFile(filename,"rb")

def list_bam_files(directory_path):
    files = []
    for filename in os.listdir(directory_path):
        if filename[-3:] == 'bam':
            file_path = os.path.join(directory_path, filename)
            try:
                if os.path.isfile(file_path):
                    files.append(get_bam_file(file_path))
            except Exception as e:
                print e
    return files

def main():
    container_name = '1000-genomes-dataset'
    config = {
    'user': INSERT_USERNAME,
    'key': INSERT_PASSWORD,
    'tenant_name': 'c2016015',
    'authurl': 'http://130.238.29.253:5000/v3'
    }
    samples = sc.parallelize(['NA19256.chrom20.ILLUMINA.bwa.YRI.low_coverage.20130415.bam', 'NA19257.chrom20.ILLUMINA.bwa.YRI.low_coverage.20130415.bam'])
    bam_files = samples.map(lambda s: download_container(config, container_name, s))
    print "\nDownloaded ", bam_files.count(), " bam_files\n"

    # Initializing
    kmers = []
    total_count = 0
    count = 0
    kmers_in_bam = []

    for b in bam_files:
            f = open('sample.bam', 'wb')
            f.write(b)
            f.close()
            bam = get_bam_file('/home/ubuntu/LDSA2016/sample.bam')
            count, kmrs_in_bam = parse_bam_file(bam)
            total_count += count
            kmers = kmers + kmrs_in_bam

    n = 20
    print 'First ', n, 'K-mers: \t', kmers[:n], '\n'
    print 'Total count: \t' + str(total_count), '\n'
    print 'Length of kmers list: \n' + str(len(kmers)), '\n'

if __name__ == '__main__':
    main()
