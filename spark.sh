#!/bin/bash

if [ "$#" -lt 3 ]; then
	echo "Usage: spark.sh (yarn|local) ClassName path/to/app.assembly.jar num_nodes_to_use [ADDITIONAL_PROGRAM_ARGS]"
    exit 1
fi

mode="$1"
class="$2"
jarPath="$3"
nodes=`expr $4`
# max 3 executors per node
executor_per_node=`expr 3`
memory_per_node=`expr 64`
num_executors=`expr $nodes \* $executor_per_node - 1`
executor_memory_per_node=`expr $memory_per_node - 1`
executor_memory=`expr $executor_memory_per_node / $executor_per_node`
executor_memory=`expr 10 \* $executor_memory - 10 \* $executor_memory \* 7 / 100`
executor_memory=`expr $executor_memory / 10`G
# max 5 cores per executor
executor_cores=`expr 4`
spark_submit="/usr/bin/spark-submit"

# remove all used arguments
shift; shift; shift; shift

if [ "$mode" = "local" ]; then
    HADOOP_USER_NAME="bp2016n1" $spark_submit --class $class --master local[*] $jarPath $*
elif [ "$mode" = "yarn" ]; then
    HADOOP_USER_NAME="bp2016n1" $spark_submit --class $class --master yarn --num-executors $num_executors --executor-cores $executor_cores --executor-memory $exec_memory $jarPath $*
else
 	echo "Unsupported mode $mode, use local or yarn"
 	echo "Usage: spark.sh (yarn|local) ClassName path/to/app.assembly.jar"
fi
