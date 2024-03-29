mkdir yelp
sudo mount -t vboxsf yelp ~/yelp/
# hadoop dfsadmin -safemode leave
sudo -u hdfs hadoop fs -mkdir /user/DataSources
sudo -u hdfs hadoop fs -put business.json checkin.json photo.json review.json tip.json user.json /user/DataSources
sudo -u hdfs hadoop fs -put GeoDataSet_2019_11_25.json  /user/DataSources
sudo -u hdfs hadoop fs -mkdir /user/Analysis1
sudo -u hdfs hadoop fs -put  result1.txt  /user/Analysis1
sudo -u hdfs hadoop fs -mkdir /user/Analysis2
sudo -u hdfs hadoop fs -put  result2.txt  /user/Analysis2
sudo -u hdfs hadoop fs -mkdir /user/Analysis3
sudo -u hdfs hadoop fs -put  result3.txt  /user/Analysis3
sudo -u hdfs hadoop fs -mkdir /user/Logs
sudo -u hdfs hadoop fs -put resultsAnalysis1_25_11_2019.log resultsAnalysis2_25_11_2019.log resultsAnalysis3_25_11_2019.log DownloadSources_25_11_2019.log  /user/Logs
