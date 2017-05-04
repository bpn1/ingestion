#!/bin/bash

nice -n 10 python /home/luigi/pipeline/textmining_pipeline.py --scheduler-host localhost | tee -a pipeline.log
