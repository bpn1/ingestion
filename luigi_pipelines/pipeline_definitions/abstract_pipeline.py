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
