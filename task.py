import pysam, os

K = 10




def count_kmers(bam):
    indices = range(bam.query_alignment_start, bam.query_alignment_end - (K-1))
    kmers = []
    for index in indices:
        kmers.append((bam.reference_start + index, bam.query_sequence[index:index+K]))
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
    for bam in list_bam_files('/home/ubuntu/'):
        count, kmers = parse_bam_file(bam)
        print count, kmers
    return

if __name__ == '__main__':
    main()
