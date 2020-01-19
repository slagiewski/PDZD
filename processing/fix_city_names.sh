sudo python3 ./fix_city_names.py
sudo python3 ./convert.py
sudo -u hdfs hadoop fs -put -f business_fixed.csv  /user/DataSources
