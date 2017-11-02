#!/bin/bash

uri_file="ttluris.txt"
output_file="dbpedia.ttl"

echo "-> Removing $output_file"
rm $output_file

while read uri; do
    echo "-> Acquiring $uri..."
    filename=$(basename $uri)

    # Only download file if not already present
    if ! [ -f $filename ]; then
        wget $uri
    fi

    echo "-> Received $filename"

    # unzip and concatenate all files
    bzcat $filename >> $output_file
    rm $filename
done <$uri_file

echo "-> Done!"
