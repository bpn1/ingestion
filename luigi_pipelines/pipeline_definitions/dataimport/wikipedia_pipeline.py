from pipeline_definitions.abstract_pipeline import AbstractPipeline
from models.task_definition import TaskDefinition

task_definitions = [TaskDefinition("WikipediaImport")]


class WikipediaPipeline(AbstractPipeline):
    name = "Wikipedia Pipeline"
    package = "de.hpi.ingestion.dataimport.wikipedia"
    task_definitions = task_definitions
