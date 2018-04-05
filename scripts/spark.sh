#!/bin/bash
usage="Usage spark.sh [OPTIONS] your.jar [PROGRAM_ARGS]"
if [ "$#" -lt 1 ]; then
    echo $usage
    echo "Use -h for help"
    exit 1
fi

while getopts ":hm:c:n:d:e:tj:x" opt; do
    case $opt in
        h)
            echo $usage
            echo -e "-m <mode>\tmode to use (yarn|shell|print)"
            echo -e "-c <class>\tclass to execute"
            echo -e "-n <num_nodes>\tnumber of nodes to use"
            echo -e "-d <num_gb>\tnumber of GB used as driver memory"
            echo -e "-e <num_gb>\tnumber of GB used as executor memory"
            echo -e "-t\t\tincrease executor stack size for trie deserialization"
            echo -e "-j <jars>\textra jars to add to the classpath (comma separated list)"
            echo -e "-x\t\tuse a node of the cluster as driver (deploy mode cluster)"
            exit 1
            ;;
        m)
            mode="$OPTARG"
            ;;
        c)
            class="$OPTARG"
            ;;
        n)
            nodes=`expr $OPTARG`
            ;;
        d)
            driver_mem="--driver-memory ${OPTARG}G"
            ;;
        e)
            exec_mem="--executor-memory ${OPTARG}G"
            ;;
        t)
            trie_option="--conf spark.executor.extraJavaOptions=-XX:ThreadStackSize=1000000"
            ;;
        j)
            jar_option="--jars $OPTARG"
            ;;
        x)
            deploy_mode="--deploy-mode cluster"
            ;;
        \?)
            echo "Invalid option: -$OPTARG" >&2
            exit 1
            ;;
        :)
            echo "Option -$OPTARG requires an argument." >&2
            exit 1
            ;;
    esac
done

shift $((OPTIND - 1))
jarPath="$1"
shift

if [ -z "$jarPath" ]; then
    echo "Path to .jar not set"
    exit 1
fi
if [ -z "$nodes" ]; then
    nodes="8"
fi

## max 3 executors per node
executor_per_node=`expr 3`
memory_per_node=`expr 64`
num_executors=`expr $nodes \* $executor_per_node - 1`
executor_memory_per_node=`expr $memory_per_node - 1`
executor_memory=`expr $executor_memory_per_node / $executor_per_node`
executor_memory=`expr 10 \* $executor_memory - 10 \* $executor_memory \* 7 / 100`
executor_memory=`expr $executor_memory / 10`G
# max 5 cores per executor
executor_cores=`expr 4`

if [ -z "$exec_mem" ]; then
    exec_mem="--executor-memory $executor_memory"
fi

job_runner="--class de.hpi.ingestion.framework.JobRunner"
exec_cores="--executor-cores $executor_cores"
num_exec="--num-executors $num_executors"
submit_command="spark-submit $job_runner $num_exec $exec_cores $exec_mem $driver_mem $trie_option $jarPath $class $*"
export HADOOP_USER_NAME="ingestion"
if [ "$mode" = "yarn" ]; then
    spark-submit $job_runner $jar_option $deploy_mode $num_exec $exec_cores $exec_mem $driver_mem $trie_option $jarPath $class "$@"
elif [ "$mode" = "shell" ]; then
    spark-shell $num_exec $exec_cores $exec_mem $driver_mem $trie_option --jars $jarPath "$@"
elif [ "$mode" = "print" ]; then
    echo "HADOOP_USER_NAME=\"ingestion\"" $submit_command
else
    echo "Unsupported mode. Use -h for help"
fi
