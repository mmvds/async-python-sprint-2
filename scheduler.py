import json
import time
import logging

from job import Job

logger = logging.getLogger(__name__)


class Scheduler:
    """
    Class to schedule Job class tasks with some checks
    """

    def __init__(self, pool_size: int = 10):
        """
        param pool_size: max amount of tasks in Scheduler
        """
        self.pool_size = pool_size
        self.tasks = []
        self.waiting_tasks = []
        self.is_running = True

    def schedule(self, task: Job):
        """
        :param task: Scheduled Job class task
        """
        if len(self.tasks) < self.pool_size:
            self.tasks.append(task)
            logger.debug(f'{task.func_name}({task.args}, {task.kwargs}) task scheduled')
        else:
            logger.error(f'Cant add new task. Max number of tasks reached ({self.pool_size})')

    def generate_summary_table(self) -> dict:
        summary = {}
        for task in self.tasks:
            summary[task.state['status']] = summary.get(task.state['status'], 0) + 1

        for status, count in summary.items():
            logger.info(f'{status}: {count}')
        return summary

    def run(self) -> dict:
        while self.is_running:
            self.waiting_tasks = [task for task in self.tasks if task.state['status'] == 'waiting']
            if not self.waiting_tasks:
                logger.info('All tasks were finished:')
                summary = self.generate_summary_table()
                self.stop()
                return summary
            else:
                for task in self.waiting_tasks:
                    job_coroutine = task.run()
                    next(job_coroutine)
            time.sleep(0.1)

    def restart(self, filename: str = 'statuses.json'):
        """
        :param filename: file name with saved tasks states
        """
        try:
            with open(filename, 'r') as f:
                states = json.load(f)
            logger.info('Scheduler was restarted, statuses were loaded')
        except (FileNotFoundError, json.JSONDecodeError) as err:
            logger.error(f'Cant find {filename} file with statuses:\n {err}')
            states = []

        for ts in states:
            for task in self.tasks:
                if task.func_name == ts['func_name'] and task.args == ts['args'] and task.kwargs == ts['kwargs']:
                    task.load_state(ts)
        self.is_running = True
        self.run()

    def stop(self, filename: str = 'statuses.json'):
        """
        :param filename: file name to save tasks states
        """
        self.is_running = False
        state = [task.state for task in self.tasks]
        try:
            with open(filename, 'w') as f:
                json.dump(state, f)
        except (FileNotFoundError, IOError) as err:
            logger.error(f'Failed to write JSON file: {err}')
        logger.info('Scheduler stopped')
