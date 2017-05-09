from textmining_pipeline import TextminingPipeline
from wikidata_pipeline import WikiDataPipeline
import luigi

if __name__ == "__main__":
    try:
        luigi.run(main_task_cls=WikiDataPipeline.get_pipeline())
    except KeyboardInterrupt:
        print("Pipeline interrupted: You have to stop the spark job manually!")
        # todo: send ctrl+c via ssh
