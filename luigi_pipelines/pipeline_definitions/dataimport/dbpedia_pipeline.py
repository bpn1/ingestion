"""
Copyright 2016-17, Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

from pipeline_definitions.abstract_pipeline import AbstractPipeline
from models.task_definition import TaskDefinition
from implisense_pipeline import ImplisensePipeline

default_package = "de.hpi.ingestion.dataimport.dbpedia"

# https://github.com/bpn1/ingestion/wiki/DBpedia-Pipeline
implisense_last_task = ImplisensePipeline.task_definitions[-1].name
task_definitions = [TaskDefinition("DBpediaImport", [implisense_last_task], package=default_package),
                    TaskDefinition("DBpediaDataLakeImport", ["DBpediaImport"], package=default_package,
                                   jar_attribute="companies.jar"),
                    TaskDefinition("DBpediaRelationParser", ["DBpediaDataLakeImport"], package=default_package),
                    TaskDefinition("DBpediaRelationImport", ["DBpediaRelationParser"], package=default_package),

                    TaskDefinition("DBpediaDeduplication", ["DBpediaRelationImport"], "Deduplication",
                                   package="de.hpi.ingestion.deduplication"),
                    TaskDefinition("DBpediaMerging", ["DBpediaDeduplication"], "Merging",
                                   package="de.hpi.ingestion.datamerge"),
                    TaskDefinition("DBpediaMasterConnecting", ["DBpediaMerging"], "MasterConnecting",
                                   package="de.hpi.ingestion.datamerge")]


class DBpediaPipeline(AbstractPipeline):
    name = "DBpedia Pipeline"
    package = default_package
    task_definitions = task_definitions
