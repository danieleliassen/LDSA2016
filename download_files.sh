# * This script downloads all files listed in BAM_ONLY.txt
# * Processes it to our intermediate form through the python program 'process_file.py'
# * saves it to the directory /mnt/volume/PROCESSED_FILES/PROCESSED_<file_name>

cat BAM_ONLY.txt | while read line
do
   wget "http://130.238.29.253:8080/swift/v1/1000-genomes-dataset/"$line
   python process_file.py $line > "/mnt/volume/PROCESSED_FILES/PROCESSED_"$line
   rm $line
# download file
   # create processed version
   # delete downloaded file
done
