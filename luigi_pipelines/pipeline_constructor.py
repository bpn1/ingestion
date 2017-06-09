from models.ingestion_task import IngestionTask


class PipelineConstructor(object):
    def __init__(self, package, task_definitions):
        self.package = package
        self.task_definitions = task_definitions
        for task in task_definitions:
            task.package = self.package

    @staticmethod
    def define_task_dictionary(tasks):
        # task names must be unique!
        task_dict = {}
        for task in tasks:
            task_dict[task.name] = task
        return task_dict

    def construct_task_dag(self, force_execution):
        tasks = PipelineConstructor.define_task_dictionary(self.task_definitions)
        last_task = None
        completed_tasks = set()
        while len(completed_tasks) < len(tasks):
            for task in tasks.values():
                if task.name in completed_tasks:
                    continue
                upstream_task_missing = False
                upstream_tasks = [tasks[upstream_task].luigi_task for upstream_task in task.upstream_tasks]
                for upstream_task in upstream_tasks:
                    if upstream_task is None:
                        upstream_task_missing = True
                        break
                if not upstream_task_missing:
                    last_task = IngestionTask(name=task.name, command=task.command, upstream_tasks=upstream_tasks,
                                              force=force_execution)
                    task.luigi_task = last_task
                    completed_tasks.add(task.name)
        return last_task
