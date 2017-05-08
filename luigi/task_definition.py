ingestion_jar = "/home/bp2016n1/jars/jenkins/ingestion_master.jar"


class TaskDefinition(object):
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
        return "ssh bp2016n1@sopedu \"/home/bp2016n1/scripts/spark.sh yarn " + self.package + "." + self.scala_class \
               + " 8 " + self.jar + "\""

    @property
    def command(self):
        if self._command is None:
            return self.create_command()
        else:
            return self._command

    @command.setter
    def command(self, value):
        self._command = value
