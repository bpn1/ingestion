ingestion_jar = "/home/bp2016n1/jars/jenkins/ingestion_master.jar"


class TaskDefinition(object):
    def __init__(self, name, upstream_tasks=None, scala_class=None, jar=ingestion_jar):
        self.name = name
        self.jar = jar
        self.luigi_task = None

        if upstream_tasks is None:
            self.upstream_tasks = []
        else:
            self.upstream_tasks = upstream_tasks

        if scala_class is None:
            self.scala_class = name
        else:
            self.scala_class = scala_class
