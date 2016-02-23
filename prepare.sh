#!/bin/bash

sudo yum install -y numpy

cd $HOME

git clone https://github.com/bernhard-42/Spark-Masterclass.git

cd Spark-Masterclass/

#
# European Union indicator Data
#

gzip -d europe-indicators.csv.gz
hdfs dfs -put europe-indicators.csv /tmp


#
# Iris.data
#
hdfs dfs -put iris.data /tmp

