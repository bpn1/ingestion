from textmining_pipeline import TextminingPipeline
from wikidata_pipeline import WikiDataPipeline
import luigi

# usage: [nice -n 19] python3 ingestion_pipeline.py --scheduler-host localhost (TextminingPipeline|WikiDataPipeline)
if __name__ == "__main__":
    try:
        luigi.run()
    except KeyboardInterrupt:
        print("Pipeline interrupted: You have to stop the spark job manually!")
        # todo: send ctrl+c via ssh