import logging
import os
from job import Job
from scheduler import Scheduler
import requests
from requests.exceptions import RequestException, HTTPError, ConnectionError

logger = logging.getLogger(__name__)


def make_dirs(dir_names: list) -> None:
    """
    param dir_names: list of directories to create
    :return:
    """
    for dir_name in dir_names:
        try:
            os.makedirs(dir_name)
            logger.info(f'Directory {dir_name} created')
        except FileExistsError:
            logger.debug(f'Directory {dir_name} already exists')
        except OSError as err:
            logger.error(f'Cant create {dir_name} directory:\n {err}')


def download_urls(sites: list, save_dir: str) -> None:
    """
    :param sites: list of sites to download main page
    :param save_dir: main directory to save sites info
    :return:
    """
    for site in sites:
        file_path = f'{save_dir}/{site}/index.html'
        try:
            response = requests.get(f'https://{site}')
            with open(file_path, 'wb') as file:
                file.write(response.content)
            logger.info(f'Saved: {file_path}')
        except (RequestException, HTTPError, ConnectionError) as err:
            logger.error(f'Cant save {site} to {file_path}:\n {err}')


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    main_dir = 'tests'
    websites = ['ya.ru', 'google.com', 'author.today']

    scheduler = Scheduler(pool_size=10)

    job1 = Job(make_dirs,
               args=([main_dir],),
               max_working_time=-1,
               tries=0,
               dependencies=None)
    job2 = Job(make_dirs,
               args=([f'{main_dir}/{website}' for website in websites],),
               max_working_time=1,
               tries=0,
               dependencies=[job1])

    job3 = Job(download_urls,
               args=(websites, main_dir),
               max_working_time=60,
               tries=3,
               dependencies=[job1, job2])

    scheduler.schedule(job1)
    scheduler.schedule(job2)
    scheduler.schedule(job3)
    scheduler.run()

    # Ok, let's emulate we forgot one more site
    forgot_website = 'litres.ru'
    job4 = Job(make_dirs,
               args=([f'{main_dir}/{forgot_website}'],),
               max_working_time=1,
               tries=0,
               dependencies=[job1])
    job5 = Job(download_urls,
               args=([forgot_website], main_dir),
               max_working_time=10,
               tries=2,
               dependencies=[job1, job4])
    scheduler.schedule(job4)
    scheduler.schedule(job5)
    scheduler.restart()
