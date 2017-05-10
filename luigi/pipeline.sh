#!/bin/bash

BASEDIR=$(dirname "$0")

usage="Usage: pipeline.sh (TextminingPipeline|WikiDataPipeline)"
if [ "$#" -ne 1 ]; then
        echo $usage
    exit 1
fi

python3 $BASEDIR/pipeline.py --scheduler-host localhost $1 | tee pipeline.log
