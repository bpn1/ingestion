from pipeline_constructor import PipelineConstructor
import luigi


class AbstractPipeline(luigi.Task):
    def __init__(self, *args, **kwargs):
        super(AbstractPipeline, self).__init__(*args, **kwargs)

    def requires(self):
        return type(self).upstream_task

    def output(self):
        return self.input()

    @classmethod
    def define_upstream_task(cls):
        cls.upstream_task = PipelineConstructor(cls.package, cls.task_definitions).construct_task_dag(
            cls.force_execution)
