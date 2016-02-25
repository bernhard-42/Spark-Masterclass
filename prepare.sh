#!/usr/bin/env bash

export HOME=${HOME:-/root}
export TERM=${TERM:-xterm}

sudo yum install -y -q numpy

cd

if ambari-server status; then
    git clone https://github.com/bernhard-42/Spark-Masterclass.git
    cd Spark-Masterclass/

    ## European Union indicator Data
    gzip -d europe-indicators.csv.gz
    hdfs dfs -put europe-indicators.csv /tmp

    ## Iris.data
    hdfs dfs -put iris.data /tmp
else
    echo "Data load to HDFS failed"
fi

