#!/bin/bash

BASEDIR=$(dirname "$0")

nice -n 10 python3 $BASEDIR/pipeline.py --scheduler-host localhost | tee pipeline.log
