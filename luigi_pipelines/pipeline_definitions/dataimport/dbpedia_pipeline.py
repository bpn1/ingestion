from pipeline_definitions.abstract_pipeline import AbstractPipeline
from models.task_definition import TaskDefinition
import luigi

# https://github.com/bpn1/ingestion/wiki/DBpedia-Pipeline
task_definitions = [TaskDefinition("DBpediaImport"),
                    TaskDefinition("DBpediaDataLakeImport", ["DBpediaImport"])]


class DBpediaPipeline(AbstractPipeline):
    name = "DBpedia Pipeline"
    force_execution = luigi.BoolParameter(default=False)
    package = "de.hpi.ingestion.dataimport.dbpedia"
    task_definitions = task_definitions

    def __init__(self, *args, **kwargs):
        super(DBpediaPipeline, self).__init__(*args, **kwargs)
        type(self).define_upstream_task()
