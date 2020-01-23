# This script compiles map reduce jobs
# It should create all JARs in the "out" dir.
set -eu
CLASS_PATH="./lib/hadoop-common-2.7.4.jar:./lib/hadoop-mapreduce-client-core-2.7.4.jar:."

javac -cp $CLASS_PATH src/PhotosAdvanced.java -d out 
cd out
jar cvf photosadvanced.jar *.class
