from pipeline_definitions.abstract_pipeline import AbstractPipeline
from models.task_definition import TaskDefinition
import luigi

task_definitions = [TaskDefinition("WikipediaImport")]


class WikipediaPipeline(AbstractPipeline):
    name = "Wikipedia Pipeline"
    force_execution = luigi.BoolParameter(default=False)
    package = "de.hpi.ingestion.dataimport.wikipedia"
    task_definitions = task_definitions

    def __init__(self, *args, **kwargs):
        super(WikipediaPipeline, self).__init__(*args, **kwargs)
        type(self).define_upstream_task()
