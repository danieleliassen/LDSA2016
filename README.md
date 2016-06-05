# LDSA2016
> Group project for the course Large Datasets for Scientific Applications, Spring 2016.



# Prerequisites
* 2+ instaces on an OpenStack-based cloud system (Preferebly running Ubuntu 14.X)
* pysam
* Apache Spark
* python-swiftclient
* python-keystoneclient
* exported username, password, api_url for swift

# Running it
> The commands below assume that you already have set up a spark cluster and installed the prerequisites

```
git clone https://github.com/adamruul/LDSA2016.git
sudo ./spark-submit --master spark://pmo:7077 --driver-memory 6g --executor-memory 2g ~/LDSA2016/main.py
```

# Authors
* Daniel Eliassen
* Octave Mariotti
* Adam Ruul
* Marcus Windmark
