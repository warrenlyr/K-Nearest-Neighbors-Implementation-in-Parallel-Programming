#!/bin/sh

spark-submit --class SparkKNN --master spark://cssmpi5h:58240 --executor-memory 4G --total-executor-cores 4 SparkKNN.jar 10