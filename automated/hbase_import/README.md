# YELP Hbase importer

## Requirements

* Python 3 (tested on 3.6.8)
* pip (pip3)
* (optional) virtualenv & virtualenvwrapper

## Installation


If there's both `pip` and `pip3` on your system, use `pip3` instead of `pip`.

`pip install -r requirements.txt`

## Running
1. Prepare `config.py`
2. Run `./hbase_import.py`


## Getting json files into hdfs
```
$ cd /path/with/dataset
$ NAMENODE=<hdfs namenode container name or id, e.g. from docker ps>
$ docker exec $NAMENODE mkdir /data
$ ls *.json | xargs  -I {} docker cp {} $NAMENODE:/data/{}\
$ docker exec -it $NAMENODE bash
$ <skrypt_hdfs>
```