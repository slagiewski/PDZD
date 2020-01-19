export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar

hadoop com.sun.tools.javac.Main PhotosAdvanced.java
jar cf pa.jar PhotosAdvanced*.class