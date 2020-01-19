sudo -u hdfs hadoop fs -cp /user/DataSources/photo.csv /user/Analysis2/MapReduce/input
sudo -u hdfs hadoop fs -rmr /user/Analysis2/MapReduce/output

sudo -u hdfs hadoop jar pa.jar PhotosAdvanced /user/Analysis2/MapReduce/input /user/Analysis2/MapReduce/output
sudo -u hdfs hadoop fs -cp -f /user/Analysis2/MapReduce/output/part-r-00000 /user/Analysis2/middle_set_photos_count.csv