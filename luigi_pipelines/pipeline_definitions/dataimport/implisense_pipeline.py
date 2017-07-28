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

default_package = "de.hpi.ingestion.dataimport.implisense"

# https://github.com/bpn1/ingestion/wiki/Implisense-Pipeline
task_definitions = [TaskDefinition("ImplisenseParser", package=default_package),
                    TaskDefinition("ImplisenseDataLakeImport", ["ImplisenseParser"], package=default_package,
                                   jar_attribute="companies.jar"),

                    TaskDefinition("ImplisenseMerging", ["ImplisenseDataLakeImport"], "Merging",
                                   package="de.hpi.ingestion.datamerge")]


class ImplisensePipeline(AbstractPipeline):
    name = "Implisense Pipeline"
    package = default_package
    task_definitions = task_definitions
