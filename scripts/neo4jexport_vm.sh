#!/bin/bash

# neo4j vars
neo4j_service="/etc/init.d/neo4j"
neo4j_db="subject"

# filenames
neo4j_dir="/home/ingestion/neo4jdata"
node_header="$neo4j_dir/nodeheader"
node_file="$neo4j_dir/nodes.csv"
edge_header="$neo4j_dir/edgeheader"
edge_file="$neo4j_dir/edges.csv"

# move to directory that allows neo4j import
cd scripts

# stop neo4j service
sudo ${neo4j_service} stop

# delete the existing neo4j database
sudo -u neo4j rm -r /var/lib/neo4j/data/databases/${neo4j_db}.db/

# import the new data to a database with the same name
sudo -u neo4j neo4j-admin import --mode=csv --database ${neo4j_db}.db --nodes "$node_header,$node_file" --relationships "$edge_header,$edge_file"

# start neo4j service
sudo ${neo4j_service} start

# delete files
rm ${node_file}
rm ${edge_file}
