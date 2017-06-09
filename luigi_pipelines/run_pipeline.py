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
        # todo: send ctrl+c via ssh
