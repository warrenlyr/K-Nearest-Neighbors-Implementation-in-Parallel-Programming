#!/bin/sh

rm *.class
javac -cp ~/hadoop-0.20.2/hadoop-0.20.2-core.jar KNearestNeighbors.java
jar -cvf KNearestNeighbors.jar *.class
javac AccuracyCalc.java