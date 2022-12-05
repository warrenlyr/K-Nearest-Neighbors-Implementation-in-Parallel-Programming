#!/bin/sh

/usr/lib/jvm/java-1.8.0/bin/javac -cp /home/NETID/css534/programming/Spark/jars/spark-core_2.11-2.3.1.jar:/home/NETID/css534/programming/Spark/jars/spark-sql_2.11-2.3.1.jar:/home/NETID/css534/programming/Spark/jars/scala-library-2.11.8.jar:google-collections-1.0.jar *.java
/usr/lib/jvm/java-1.8.0/bin/jar -cvf SparkKNN.jar *.class