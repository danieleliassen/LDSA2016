import pysam, os, sys
import swiftclient.client
from cStringIO import StringIO
K = 10

def download_bam_from_swift():
    config = {
    'user':os.environ['OS_USERNAME'],
    'key':os.environ['OS_PASSWORD'],
    'tenant_name':os.environ['OS_TENANT_NAME'],
    'authurl':os.environ['OS_AUTH_URL']
    }
    container_name = '1000-genomes-dataset'
    conn = swiftclient.client.Connection(auth_version=3, **config)
    (storage_url, auth_token) = conn.get_auth()
    (response, content) = swiftclient.client.get_container(url=storage_url,container=container_name, token=auth_token)
    bam_files = []
    #### SAMPLES####
    samples = ['NA19256.chrom20.ILLUMINA.bwa.YRI.low_coverage.20130415.bam', 'NA19257.chrom20.ILLUMINA.bwa.YRI.low_coverage.20130415.bam']
    ################
    for bucket in content:
        filename = bucket['name']
        if filename[-3:] == 'bam' and filename in samples:
                response_header, file_contents = conn.get_object(container_name, filename)
                bam_files.append(file_contents)
    return bam_files

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
    bam_files = download_bam_from_swift()
    print "Downloaded ", len(bam_files), " bam_files"

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
            print "Kmers while looping: ", kmrs_in_bam[5:]

    print 'Total count: ' + str(total_count)
    print 'Length of kmers list: ' + str(len(kmers))
    print 'Some K-mers: ', kmers
    #for bam in list_bam_files('/home/ubuntu/'):
    #    count, kmers = parse_bam_file(bam)
        #print count, kmers
    for bamfile in list_bam_files('/home/ubuntu/LDSA2016/'):
        c, ks = parse_bam_file(bamfile)
        print c, ks

    return

if __name__ == '__main__':
    main()
