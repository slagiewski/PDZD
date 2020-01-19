export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar
hadoop com.sun.tools.javac.Main PhotosCount.java
jar cf wc.jar PhotosCount*.class

hadoop com.sun.tools.javac.Main CompletePhotosCount.java
jar cf wcc.jar CompletePhotosCount*.class

#hadoop com.sun.tools.javac.Main PhotosAdvanced.java
#jar cf wca.jar PhotosAdvanced*.class
