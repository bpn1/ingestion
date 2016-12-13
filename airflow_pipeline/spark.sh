#!/bin/bash

usage="Usage: spark.sh (yarn|local) ClassName path/to/app.assembly.jar [ADDITIONAL_SPARK_ARGS]"

if [ "$#" -lt 3 ]; then
	echo $usage
	exit 1
fi

mode="$1"
class="$2"
jarPath="$3"
spark_submit="/usr/local/spark/bin/spark-submit"

# remove all used arguments
shift; shift; shift

if [ "$mode" = "local" ]; then
	$spark_submit --class $class --master local[*] $* $jarPath
elif [ "$mode" = "yarn" ]; then
	# --conf spark.executor.instances=8
	$spark_submit --class $class --master yarn --executor-cores 4 --executor-memory 11060M --driver-memory 8g $* $jarPath
else
	echo "Unsupported mode $mode, use local or yarn"
	echo $usage	