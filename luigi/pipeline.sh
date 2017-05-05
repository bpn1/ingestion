#!/bin/bash

nice -n 10 python3 /home/luigi/pipeline/textmining_pipeline.py --scheduler-host localhost | tee pipeline.log
