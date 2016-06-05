# LDSA2016
> Group project for the course Large Datasets for Scientific Applications, Spring 2016.



# Prerequisites
* 2+ instaces on an OpenStack-based cloud system (Preferebly running Ubuntu 14.X)
* pysam
* Apache Spark
* python-swiftclient
* python-keystoneclient
* config.py (defined as below)
```python
# config.py used to provide swiftclient with authentication.
# Only due to issues with resolving environmental variables in Spark.

def main():
  username = "hardcoded"
  password = "hardcoded"
	return { 'user':username,'key':password,'tenant_name':'c2016015','authurl': 'http://130.238.29.253:5000/v3'}

if __name__ == '__main__':
    main()
```
# Running it
> The commands below assume that you already have set up a spark cluster and installed the prerequisites

```
git clone https://github.com/adamruul/arduino-project.git
sudo ./spark-submit --master spark://pmo:7077 --driver-memory 6g --executor-memory 2g ~/LDSA2016/main.py
```

# Authors
* Daniel Eliassen
* Octave Mariotti
* Adam Ruul
* Marcus Windmark
