# LDSA2016
Group repo for the project Large datasets for scientific applications. Spring 2016

# Running it
```
sudo pyspark main.py [ -f FILE_WITH_URLS | -p LOCAC_PATH_TO_FILES ]
# Results are saved in the directory $HOME/RESULTS/
```

## Various Resources
Here's some of the information we have found.
### K-mers

**K-mers (Video)**
https://www.youtube.com/watch?v=2UsmUgJtwAI
https://www.youtube.com/watch?v=My_sw_Rf_4U

**Efficient K-mers counting (Whitepaper)**
http://bmcbioinformatics.biomedcentral.com/articles/10.1186/1471-2105-12-333

**Scala-snippet for extracting K-mers**

```scala
def getMappedKmers(k: Int, seq: String): IndexedSeq[Map[String, Int]] = {
  for (i <- 0 until seq.length - k) yield {
    Map(seq.substring(i, i + k) -> 1) // Map the k-mer to a count of 1
  }
}
```

### BAM format

**SAM Tools' SAM/BAM specification**
https://samtools.github.io/hts-specs/SAMv1.pdf
