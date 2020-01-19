sudo -u hdfs hadoop fs -cp /user/DataSources/photo.csv /user/Analysis2/MapReduce/input
sudo -u hdfs hadoop fs -rmr /user/Analysis2/MapReduce/output

sudo -u hdfs hadoop jar wc.jar PhotosCount /user/Analysis2/MapReduce/input /user/Analysis2/MapReduce/output
sudo -u hdfs hadoop fs -cp /user/Analysis2/MapReduce/output/part-r-00000 /user/Analysis2/middle_set_photos_per_category.csv

sudo python3 filter_out_invalid_records.py
sudo -u hdfs hadoop fs -put -f middle_set_photos_per_category.csv /user/Analysis2/middle_set_photos_per_category.csv

sudo -u hdfs hadoop fs -cp /user/Analysis2/middle_set_photos_per_category.csv /user/Analysis2/MapReduceComplete/input
sudo -u hdfs hadoop fs -rmr /user/Analysis2/MapReduceComplete/output

sudo -u hdfs hadoop jar wcc.jar CompletePhotosCount /user/Analysis2/MapReduceComplete/input /user/Analysis2/MapReduceComplete/output
sudo -u hdfs hadoop fs -cp /user/Analysis2/MapReduceComplete/output/part-r-00000 /user/Analysis2/middle_set_photos_complete.csv