# THIS SCRIPT DOWNLOADS ALL THE FILENAMES OF THE .bam FILES AND STORES THEM IN THE FILE 'BAM_FILENAME'
curl http://130.238.29.253:8080/swift/v1/1000-genomes-dataset > BAM_FILENAMES.txt
echo "******************************"
echo "saved list of genome files in the file 'BAM_FILENAMES.txt'"
echo "******************************"

