import pipeline_definitions
import luigi

# usage: [nice -n 19] python3 run_pipeline.py --scheduler-host localhost <PipelineName>
if __name__ == "__main__":
    try:
        luigi.run()
    except KeyboardInterrupt:
        print("Pipeline interrupted: You have to stop the spark job manually!")
        # todo: send ctrl+c via ssh