from textmining_pipeline import TextminingPipeline
import luigi

if __name__ == "__main__":
    try:
        luigi.run(main_task_cls=TextminingPipeline)
    except KeyboardInterrupt:
        print("Pipeline interrupted: You have to stop the spark job manually!")
        # todo: send ctrl+c via ssh
