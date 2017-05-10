import luigi
import os


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
