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

from models.forceable_task import ForceableTask
import luigi
import subprocess


class IngestionTask(ForceableTask):
    name = luigi.Parameter()
    command = luigi.Parameter()
    upstream_tasks = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super(IngestionTask, self).__init__(*args, **kwargs)

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.name

    def requires(self):
        return self.upstream_tasks

    def output(self):
        return luigi.LocalTarget(self.output_path())

    def output_path(self):
        prefix = "/home/luigi/pipeline/logs/"
        return prefix + self.name + ".log"

    def inputs(self):
        return [upstream_task.output() for upstream_task in self.requires()]

    def has_upstream_completed(self):
        completed = True
        for input in self.inputs():
            with input.open("r") as fin:
                status = int(fin.readline())
                completed &= status == 0
        return completed

    def execute_command(self):
        with self.output().open("w") as outfile:
            status = subprocess.call(self.command, shell=True)
            outfile.write(str(status))
            print("status: \t" + str(status))

    def _upstream_task_names(self):
        upstream_task_names = [task.name for task in self.upstream_tasks]
        upstream_task_names = "[" + ", ".join(upstream_task_names) + "]"
        return upstream_task_names

    def run(self):
        print("\n/----  Task started  -----")
        print("|name:            " + self.name)
        print("|force-execution: " + str(self.force))
        print("|requires:        " + self._upstream_task_names())
        print("|output:          " + self.output_path())
        print("|command:         " + self.command)
        if self.has_upstream_completed():
            self.execute_command()
            print("\---- Task completed -----")
        else:
            print("Error: upstream tasks " + self._upstream_task_names() + " have not all completed!")
            print("\----  Task aborted  -----")
