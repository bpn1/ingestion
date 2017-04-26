from __future__ import print_function
import luigi
import commands
import os

ingestion_jar = '/home/bp2016n1/jars/ingestion-assembly-1.0.jar'
package_prefix = 'de.hpi.ingestion.textmining.'

programs = [
    'TextParser',
    'LinkCleaner'
    'RedirectResolver'
    'LinkAnalysis',
    'LocalTrieBuilder',
    'AliasTrieSearch',  
    'AliasCounter',
    'DocumentFrequencyCounter',
    'TermFrequencyCounter',
    'CosineContextExtractor']


def create_command(program):
    return '/home/bp2016n1/scripts/spark.sh yarn ' + package_prefix + program + ' 8 ' + ingestion_jar


def make_operator_chain(programs, force_execution):
    last_task = None
    for program in programs:
        last_task = IngestionTask(name=program, command=create_command(program), upstream_task=last_task,
                                  force=force_execution)
    return last_task


def execute_command(command):
    return commands.getstatusoutput(command)


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


class IngestionTask(ForceableTask):
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
            prefix = '/home/bp2016n1/luigi/'
        return prefix + self.name + '.log'

    def has_upstream_completed(self):
        if self.requires() is None:
            return True
        with self.input().open('r') as fin:
            status = int(fin.readline())
            return status == 0

    def execute_command(self):
        with self.output().open('w') as outfile:
            status, output = commands.getstatusoutput(self.command)
            outfile.write(str(status))
            outfile.write('\n')
            outfile.write(output)
            print('status: \t' + str(status))

    def run(self):
        print('-----  Task started  -----')
        print('name:            ' + self.name)
        print('force-execution: ' + str(self.force))
        print('requires:        ' + str(self.upstream_task))
        print('output:          ' + self.output_path())
        print('command:         ' + self.command)
        if self.has_upstream_completed():
            self.execute_command()
            print('----- Task completed -----')
        else:
            print('Error: upstream task ' + str(self.upstream_task) + ' has not completed!')
            print('-----  Task aborted  -----')


class IngestionWorkflow(luigi.Task):
    name = 'IngestionWorkflow'
    force_execution = luigi.BoolParameter(default=False)
    upstream_task = make_operator_chain(programs, force_execution)

    def requires(self):
        return self.upstream_task

    def output(self):
        return self.input()


if __name__ == '__main__':
    luigi.run()
