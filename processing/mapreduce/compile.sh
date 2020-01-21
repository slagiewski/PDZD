set -eux
CLASS_PATH="./lib/hadoop-common-2.7.4.jar:./lib/hadoop-mapreduce-client-core-2.7.4.jar:."

javac -cp $CLASS_PATH src/PhotosAdvanced.java -d out 
jar -c out/ > photosadvanced.jar