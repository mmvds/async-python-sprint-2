import unittest
import logging
from unittest.mock import patch
from datetime import datetime
from job import Job, coroutine
from scheduler import Scheduler

logging.basicConfig(level=logging.DEBUG)


class JobSchedulerTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        logging.debug('Scheduler tests started...')

    @classmethod
    def tearDownClass(cls) -> None:
        logging.debug('Scheduler tests finished...')

    def test_job_run_successfully(self):
        def test_function():
            return 'Success'

        job = Job(test_function)
        job_coroutine = job.run()
        next(job_coroutine)

        result = job_coroutine.send(None)
        self.assertEqual(result['status'], 'completed')
        self.assertEqual(result['error'], '')

    def test_job_run_failed(self):
        def test_function():
            raise ValueError("Test Error")

        job = Job(test_function, tries=2)

        job_coroutine = job.run()
        next(job_coroutine)

        result = job_coroutine.send(None)
        self.assertEqual(result['status'], 'failed')
        self.assertEqual(result['error'], 'Test Error')

        result = job_coroutine.send(None)
        self.assertEqual(result['status'], 'failed')
        self.assertEqual(result['remaining_restarts'], 0)

    def test_job_with_dependencies(self):
        def dependency_function():
            return str(datetime.now())

        dependency_job = Job(dependency_function)

        def test_function():
            return "Success"

        job = Job(test_function, dependencies=[dependency_job])

        scheduler = Scheduler()
        scheduler.schedule(dependency_job)
        scheduler.schedule(job)
        scheduler.run()
        self.assertEqual(job.state['status'], 'completed')

    def test_job_with_failed_dependencies(self):
        def fail_dependency_function():
            raise ValueError("Test Error")

        dependency_job = Job(fail_dependency_function)

        def test_function():
            return "Success"

        job = Job(test_function, dependencies=[dependency_job])

        scheduler = Scheduler()
        scheduler.schedule(dependency_job)
        scheduler.schedule(job)
        scheduler.run()
        self.assertEqual(job.state['status'], 'failed')

    def test_scheduler_schedule_and_run_jobs(self):
        def test_function():
            return "Success"

        job = Job(test_function)
        scheduler = Scheduler()
        scheduler.schedule(job)
        scheduler.run()
        self.assertEqual(job.state['status'], 'completed')

    def test_scheduler_restart_jobs(self):
        def test_function():
            raise ValueError("Test Error")

        job = Job(test_function, tries=2)
        scheduler = Scheduler(pool_size=1)
        scheduler.schedule(job)
        scheduler.run()
        self.assertEqual(job.state['status'], 'failed')
        self.assertEqual(job.state['remaining_restarts'], 0)


if __name__ == "__main__":
    unittest.main(module=__name__)
