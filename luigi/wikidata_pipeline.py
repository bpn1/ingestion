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
    name = "WikiData Pipeline"
    force_execution = luigi.BoolParameter(default=False)
    package = "de.hpi.ingestion.dataimport.wikidata"
    task_definitions = task_definitions

    def __init__(self, *args, **kwargs):
        super(WikiDataPipeline, self).__init__(*args, **kwargs)
        type(self).define_upstream_task()
