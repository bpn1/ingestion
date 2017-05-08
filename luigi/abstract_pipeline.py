import luigi


class AbstractPipeline(luigi.Task):
    name = "Abstract Pipeline"
    force_execution = luigi.BoolParameter(default=False)
    tasks_dict = {}
    upstream_task = None

    def requires(self):
        return self.upstream_task

    def output(self):
        return self.input()
