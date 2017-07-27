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

from pipeline_constructor import PipelineConstructor
from models.forceable_task import ForceableTask


class AbstractPipeline(ForceableTask):
    def __init__(self, *args, **kwargs):
        self.upstream_task = None
        super(AbstractPipeline, self).__init__(*args, **kwargs)

    def requires(self):
        if self.upstream_task is None:
            self.upstream_task = self._define_upstream_task()
        return self.upstream_task

    def output(self):
        return self.input()

    def _define_upstream_task(self):
        return PipelineConstructor(self.package, self.task_definitions).construct_task_dag(self.force)
