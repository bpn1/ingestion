#!/bin/bash

# binaries
neo4j_service="/etc/init.d/neo4j"

# filenames
neo4j_dir="~/neo4jdata"
node_header="$neo4j_dir/nodeheader"
node_file="$neo4j_dir/nodes.csv"
edge_header="$neo4j_dir/edgeheader"
edge_file="$neo4j_dir/edges.csv"
neo4j_db="subject"

# verify that node and edge files exist
if [ ! -f "~/$neo4j_dir/$node_header" ]; then
    echo "Node header not found!"
    exit 1
fi

if [ ! -f "~/$neo4j_dir/$node_file" ]; then
    echo "Node file not found!"
    exit 1
fi

if [ ! -f "~/$neo4j_dir/$edge_header" ]; then
    echo "Edge header not found!"
    exit 1
fi

if [ ! -f "~/$neo4j_dir/$edge_file" ]; then
    echo "Edge file not found!"
    exit 1
fi

# stop neo4j service
sudo ${neo4j_service} stop

# delete the existing neo4j database
sudo -u neo4j rm -r /var/lib/neo4j/data/databases/${neo4j_db}.db/

# import the new data to a database with the same name
sudo -u neo4j neo4j-admin import --mode=csv --database ${neo4j_db}.db --nodes "$node_header,$node_file" --relationships "$edge_header,$edge_file"

# start neo4j service
sudo ${neo4j_service} start
