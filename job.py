import logging
import time
import json
from functools import wraps
from typing import Callable
from urllib.error import HTTPError


logger = logging.getLogger(__name__)


def coroutine(func):
    @wraps(func)
    def inner(*args, **kwargs):
        fn = func(*args, **kwargs)
        fn.send(None)
        return fn

    return inner


class Job:
    """
    Class of tasks (jobs)
    """
    def __init__(self,
                 func: Callable,
                 args: tuple = None,
                 kwargs: dict = None,
                 max_working_time: float = -1,
                 start_at: float = 0,
                 tries: int = 0,
                 dependencies: list = None):
        """
        :param func: Callable object to run
        :param args: args of callable object
        :param kwargs: kwargs of callable object
        :param max_working_time: time limit of function work in seconds
        :param start_at: unix timestamp - when should the task be run
        :param tries: max tries to restart failed function
        :param dependencies: list of dependencies jobs
        """
        self.func = func
        self.func_name = func.__name__
        self.args = args or ()
        self.kwargs = kwargs or {}
        self.max_working_time = max_working_time
        self.start_at = start_at
        self.tries = tries
        self.dependencies = dependencies or []
        self.dependencies_closed = True
        self.state = {
            'func_name': self.func_name,
            'args': self.args,
            'kwargs': self.kwargs,
            'status': 'waiting',
            'start_at': 0.0,
            'end_at': 0.0,
            'remaining_restarts': self.tries,
        }

    @coroutine
    def run(self):
        while True:
            if self.state['status'] == 'running':
                yield from self.handle_running()
            elif self.state['status'] == 'waiting':
                yield from self.handle_waiting()
            else:
                yield self.state

    @coroutine
    def handle_running(self):
        if self.state['start_at'] and -1 < self.max_working_time < (time.time() - self.state['start_at']):
            logging.info(f'{self.func_name} Job duration {self.max_working_time} exceeded')
            if self.tries > 0:
                yield self.restart()
            else:
                self.state['status'] = 'failed'
                self.state['error'] = 'timeout'
                self.state['end_time'] = time.time()
                logging.info(f'{self.func_name}. Stopping the job')
        yield self.state

    @coroutine
    def handle_waiting(self):
        self.dependencies_closed = self.check_dependencies()
        if not self.dependencies_closed or (self.dependencies_closed and self.check_start_time()):
            yield self.state
        else:
            yield from self.run_job()

    def check_dependencies(self):
        for dependency in self.dependencies:
            if dependency.state['status'] == 'failed':
                self.state['status'] = 'failed'
                self.state['error'] = 'dependent task failed'
                self.state['end_time'] = time.time()
                logging.error(f'{self.func_name} Cannot start the job, dependent task failed')
                return False
            elif dependency.state['status'] != 'completed':
                logging.error(f'{self.func_name} Cannot start the job, dependent task is not complete')
                return False
        return True

    def check_start_time(self):
        if self.start_at and time.time() < self.start_at:
            logging.error(f'{self.func_name} Job is scheduled to start at a later time')
            return True
        return False

    @coroutine
    def run_job(self):
        try:
            self.state['status'] = 'running'
            yield self.state
            result = self.func(*self.args, **self.kwargs)
            logging.info(f'{self.func_name} Job completed successfully\n with result {result}')
            self.state['status'] = 'completed'
            self.state['end_at'] = time.time()
            yield self.state
        except (HTTPError, json.JSONDecodeError, IOError, FileNotFoundError, TypeError) as err:
            logging.error(f'{self.func_name} Job failed. Error:\n {str(err)}')
            self.state['status'] = 'failed'
            self.state['error'] = str(err)
            if self.tries > 0:
                yield self.restart()
            else:
                yield self.state

    def restart(self):
        if self.state['remaining_restarts'] > 0:
            self.state['status'] = 'waiting'
            self.state['start_at'] = 0
            self.state['end_at'] = 0
            self.state['remaining_restarts'] -= 1
            logging.info(f'{self.func_name} Restarting the job, {self.state["remaining_restarts"]} tries left')
        else:
            self.state['status'] = 'failed'
            self.state['end_at'] = time.time()
            logging.error(f'{self.func_name} Job max restarts {self.tries} exceeded')
        return self.state

    def load_state(self, state):
        self.state = state
