"""
Copyright 2016-17, Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import pipeline_definitions
from pipeline_definitions.dataimport.wikidata_pipeline import WikidataPipeline
import luigi
import sys

# usage: [nice -n 19] python3 run_pipeline.py --scheduler-host localhost <PipelineName> [--force]
if __name__ == "__main__":
    try:
        if len(sys.argv) == 1:
            print("Program called without parameters: Performing dry run")
            luigi.run(["--local-scheduler"], main_task_cls=WikidataPipeline(force=True))
        else:
            luigi.run()
    except KeyboardInterrupt:
        print("Pipeline interrupted: You have to stop the spark job manually!")
