import luigi
import commands
import os

wiki_jar = '/home/hadoop/WikiImport/target/scala-2.10/WikiImport-assembly-1.0.jar'
programs = [
    ('WikiDataRDD', wiki_jar),
    ('TagEntities', wiki_jar),
    ('ResolveEntities', wiki_jar),
    ('DataLakeImport', wiki_jar),
    ('FindRelations', wiki_jar)]


def create_command(program, jar_path):
    return './spark.sh yarn ' + program + ' ' + jar_path


def make_operator_chain(programs):
    last_task = None
    for i in range(len(programs)):
        program, jar_path = programs[i]
        last_task = IngestionTask(name=program, command=create_command(program, jar_path), upstream_task=last_task,
                                  force=True)
    return last_task


class TaskHelpers():
    def execute_command(self, command):
        return commands.getstatusoutput(command)

    def x(self, command):
        """ just a short alias """
        return self.execute_command(command)


class ForceableTask(luigi.Task):
    # https://github.com/spotify/luigi/issues/595
    force = luigi.BoolParameter(significant=False, default=False)

    def __init__(self, *args, **kwargs):
        super(ForceableTask, self).__init__(*args, **kwargs)
        # To force execution, we just remove all outputs before `complete()` is called
        if self.force is True:
            outputs = luigi.task.flatten(self.output())
            for out in outputs:
                if out.exists():
                    os.remove(self.output().path)


class IngestionTask(ForceableTask, TaskHelpers):
    name = luigi.Parameter()
    command = luigi.Parameter()
    upstream_task = luigi.Parameter(default=None)

    def requires(self):
        return self.upstream_task

    def output(self):
        return luigi.LocalTarget(self.output_path())

    def output_path(self):
        if self.requires() is not None:
            prefix = self.input().path + '.'
        else:
            prefix = ''
        return prefix + self.name

    def run(self):
        print('---------------')
        print(self.name)
        print(self.command)
        print(self.upstream_task)
        print(self.output_path())
        print(self.x(self.command))
        print('---------------')
        with self.output().open('w') as outfile:
            outfile.write(self.name + ' completed!')  # dummy file, see https://github.com/spotify/luigi/issues/848


class IngestionWorkflow(luigi.Task):
    name = 'IngestionWorkflow'
    upstream_task = make_operator_chain(programs)

    def requires(self):
        return self.upstream_task

    def output(self):
        return self.input()


if __name__ == '__main__':
    luigi.run()
