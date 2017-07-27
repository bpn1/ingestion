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

class TaskDefinition(object):
    ingestion_jar = "/home/bp2016n1/jars/jenkins/ingestion_master.jar"

    def __init__(self, name, upstream_tasks=None, scala_class=None, jar=ingestion_jar, command=None, package=None):
        self.name = name
        self.jar = jar
        self.luigi_task = None
        self._command = command
        self.package = package

        if upstream_tasks is None:
            self.upstream_tasks = []
        else:
            self.upstream_tasks = upstream_tasks

        if scala_class is None:
            self.scala_class = name
        else:
            self.scala_class = scala_class

    def create_command(self):
        assert self.package is not None  # package has to be set prior to command construction
        full_class_name = self.package + "." + self.scala_class
        return "ssh bp2016n1@sopedu \"HADOOP_USER_NAME='bp2016n1' spark-submit --class " + full_class_name + \
               " --master yarn --num-executors 23 --executor-cores 4 --executor-memory 10G " + self.jar + "\""

    @property
    def command(self):
        if self._command is None:
            return self.create_command()
        else:
            return self._command

    @command.setter
    def command(self, value):
        self._command = value
