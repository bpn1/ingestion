#!/bin/bash

BASEDIR=$(dirname "$0")

USAGE="Usage: pipeline.sh <pipeline name>"
PIPELINES="Valid pipeline names are: TextminingPipeline, DBpediaPipeline, WikiDataPipeline, WikipediaPipeline"
if [ "$#" -ne 1 ]; then
        echo $USAGE
        echo $PIPELINES
    exit 1
fi

python3 $BASEDIR/run_pipeline.py --scheduler-host localhost $1 | tee pipeline.log
