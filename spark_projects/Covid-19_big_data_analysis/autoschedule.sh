#!/bin/bash
. /home/hadoop/.bashrc
changedname=`date '+%Y%m%d%H%M%S'`
cd /tmp/
export filename=`echo 'covidanalyser_'$changedname $$'.log'`
((
/usr/local/spark/bin/spark-submit --master spark://bdrenfdludcf01:7077 /home/hadoop/development/covidanalysis.py
)2>&1)| tee -a $filename
