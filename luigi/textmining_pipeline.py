from __future__ import print_function
import luigi
import subprocess
import os

# ingestion_jar = "/home/bp2016n1/jars/ingestion-assembly-1.0.jar"
ingestion_jar = "/home/bp2016n1/jars/jenkins/ingestion_feature_textmining_34.jar"
package_prefix = "de.hpi.ingestion.textmining."


class TaskDefinition(object):
    def __init__(self, task_name, upstream_tasks=None, scala_class=None):
        self.task_name = task_name

        if upstream_tasks is None:
            self.upstream_tasks = []
        else:
            self.upstream_tasks = upstream_tasks

        if scala_class is None:
            self.scala_class = task_name
        else:
            self.scala_class = scala_class


# https://docs.google.com/presentation/d/1KhNmbFrYCKe223mJjRMIJJ_CJPJX4Gk8sWdWcUwPlT4/edit?usp=sharing
tasks_definitions = [TaskDefinition("TextParser"),
                     TaskDefinition("LinkCleaner", ["TextParser"]),
                     TaskDefinition("RedirectResolver", ["LinkCleaner"]),
                     TaskDefinition("LinkAnalysis1", ["RedirectResolver"], "LinkAnalysis"),
                     TaskDefinition("CompanyLinkFilter", ["LinkAnalysis1"]),
                     TaskDefinition("LinkAnalysis2", ["CompanyLinkFilter"], "LinkAnalysis"),
                     TaskDefinition("LocalTrieBuilder", ["CompanyLinkFilter"]),
                     TaskDefinition("AliasTrieSearch", ["LocalTrieBuilder"]),
                     TaskDefinition("AliasCounter", ["AliasTrieSearch"]),
                     TaskDefinition("DocumentFrequencyCounter", ["TextParser"]),
                     TaskDefinition("TermFrequencyCounter", ["CompanyLinkFilter"]),
                     TaskDefinition("CosineContextExtractor",
                                    ["DocumentFrequencyCounter", "AliasCounter", "TermFrequencyCounter",
                                     "LinkAnalysis2"]),
                     TaskDefinition("Classifier", ["CosineContextExtractor"])]


def define_task_dictionary(tasks):
    # task names must be unique!
    task_dict = {}
    for task in tasks:
        task_dict[task.task_name] = task
    return task_dict


def create_command(program):
    return "ssh bp2016n1@sopedu \"/home/bp2016n1/scripts/spark.sh yarn " + package_prefix + program + " 8 " \
           + ingestion_jar + "\""


def make_operator_dag(tasks, force_execution):
    last_task = None
    for task in tasks.values():
        command = create_command(task.scala_class)
        upstream_tasks = [tasks[upstream_task] for upstream_task in task.upstream_tasks]
        last_task = TextminingTask(name=task.task_name, command=command, upstream_tasks=upstream_tasks,
                                   force=force_execution)
    return last_task


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


class TextminingTask(ForceableTask):
    name = luigi.Parameter()
    command = luigi.Parameter()
    upstream_tasks = luigi.Parameter(default=None)

    def requires(self):
        return self.upstream_tasks

    def output(self):
        return luigi.LocalTarget(self.output_path())

    def output_path(self):
        prefix = "/home/luigi/pipeline/logs/"
        return prefix + self.name + ".log"

    def inputs(self):
        [upstream_task.output() for upstream_task in self.requires()]

    def have_upstream_tasks_completed(self):
        completed = True
        for input in self.inputs():
            with input.open("r") as fin:
                status = int(fin.readline())
                completed &= status == 0
        return completed

    def execute_command(self):
        with self.output().open("w") as outfile:
            status, output = subprocess.call(self.command, shell=True)
            outfile.write(str(status))
            outfile.write("\n")
            outfile.write(output)
            print("status: \t" + str(status))

    def run(self):
        print("-----  Task started  -----")
        print("name:            " + self.name)
        print("force-execution: " + str(self.force))
        print("requires:        " + str(self.upstream_tasks))
        print("output:          " + self.output_path())
        print("command:         " + self.command)
        if self.has_upstream_completed():
            self.execute_command()
            print("----- Task completed -----")
        else:
            print("Error: upstream tasks " + str(self.upstream_tasks) + " have not completed!")
            print("-----  Task aborted  -----")


class TextminingWorkflow(luigi.Task):
    name = "TextminingWorkflow"
    force_execution = luigi.BoolParameter(default=False)
    tasks_dict = define_task_dictionary(tasks_definitions)
    upstream_task = make_operator_dag(tasks_dict, force_execution)

    def requires(self):
        return self.upstream_task

    def output(self):
        return self.input()


if __name__ == "__main__":
    try:
        luigi.run(main_task_cls=TextminingWorkflow)
    except KeyboardInterrupt:
        print("Pipeline interrupted")
        # todo: send ctrl+c via ssh
