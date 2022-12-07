#!/bin/sh

~/hadoop-0.20.2/bin/hadoop fs -rmr /user/yuanma/output
rm part-00000
~/hadoop-0.20.2/bin/hadoop jar KNearestNeighbors.jar KNearestNeighbors input output 10 test.csv
~/hadoop-0.20.2/bin/hadoop fs -get /user/yuanma/output/part-00000 part-00000
java AccuracyCalc