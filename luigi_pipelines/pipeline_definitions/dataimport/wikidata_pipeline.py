from pipeline_definitions.abstract_pipeline import AbstractPipeline
from models.task_definition import TaskDefinition

# https://github.com/bpn1/ingestion/wiki/Wikidata-Pipeline
task_definitions = [TaskDefinition("WikiDataImport"),
                    TaskDefinition("TagEntities", ["WikiDataImport"]),
                    TaskDefinition("ResolveEntities", ["TagEntities"]),
                    TaskDefinition("WikiDataDataLakeImport", ["ResolveEntities"]),
                    TaskDefinition("FindRelations", ["WikiDataDataLakeImport"])]


class WikidataPipeline(AbstractPipeline):
    name = "Wikidata Pipeline"
    package = "de.hpi.ingestion.dataimport.wikidata"
    task_definitions = task_definitions
