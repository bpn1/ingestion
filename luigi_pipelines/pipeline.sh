#!/bin/bash

BASEDIR=$(dirname "$0")

USAGE="Usage: pipeline.sh <pipeline name> [--force]"
PIPELINES="Valid pipeline names are: TextminingPipeline, DBpediaPipeline, WikidataPipeline, WikipediaPipeline"
if [ "$#" -lt 1 ]; then
        echo $USAGE
        echo $PIPELINES
    exit 1
fi

python3 $BASEDIR/run_pipeline.py --scheduler-host localhost $1 $2 | tee pipeline.log
