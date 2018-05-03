#!/bin/bash

# ingestionvm ip
ingestionvm="172.16.64.31"

# filenames
temp_dir="~/data"
node_dir="export_nodes"
node_file="$temp_dir/nodes.csv"
edge_dir="export_edges"
edge_file="$temp_dir/edges.csv"
neo4j_dir="neo4jdata"
scriptname="neo4jexport_vm.sh"

# create a temp dir for the csv files
mkdir ${temp_dir}

# get node data from the HDFS and delete the directory in the HDFS
hdfs dfs -getmerge ${node_dir} ${node_file}
hdfs dfs -rm -r ${node_dir}

# get edge data from the HDFS and delete the directory in the HDFS
hdfs dfs -getmerge ${edge_dir} ${edge_file}
hdfs dfs -rm -r ${edge_dir}

# send data to ingestionvm
scp ${node_file} ingestion@${ingestionvm}:~/${neo4j_dir}
scp ${edge_file} ingestion@${ingestionvm}:~/${neo4j_dir}

# delete temp dir
rm -rf ${temp_dir}

# execute script on the ingestionvm
ssh ingestion@${ingestionvm} "bash /home/ingestion/scripts/$scriptname"
