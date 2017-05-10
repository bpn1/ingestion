from pipeline_definitions.abstract_pipeline import AbstractPipeline
from models.task_definition import TaskDefinition

# https://github.com/bpn1/ingestion/wiki/DBpedia-Pipeline
task_definitions = [TaskDefinition("DBpediaImport"),
                    TaskDefinition("DBpediaDataLakeImport", ["DBpediaImport"])]


class DBpediaPipeline(AbstractPipeline):
    name = "DBpedia Pipeline"
    package = "de.hpi.ingestion.dataimport.dbpedia"
    task_definitions = task_definitions
