from abstract_pipeline import AbstractPipeline
from task_definition import TaskDefinition
import luigi

# https://github.com/bpn1/ingestion/wiki/WikiData-Pipeline
task_definitions = [TaskDefinition("WikiDataImport"),
                    TaskDefinition("TagEntities", ["WikiDataImport"]),
                    TaskDefinition("ResolveEntities", ["TagEntities"]),
                    TaskDefinition("WikiDataDataLakeImport", ["ResolveEntities"]),
                    TaskDefinition("FindRelations", ["WikiDataDataLakeImport"])]


class WikiDataPipeline(AbstractPipeline):
    @classmethod
    def get_pipeline(cls):
        cls.name = "WikiData Pipeline"
        cls.force_execution = luigi.BoolParameter(default=False)
        cls.package = "de.hpi.ingestion.dataimport.wikidata"
        cls.task_definitions = task_definitions
        cls.define_upstream_task()
        return cls
