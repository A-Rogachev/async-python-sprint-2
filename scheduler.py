import inspect
import multiprocessing
import os
import json
import random
import shutil
import sys
from time import sleep

from job import Job
from logging_setup import setup_logger
from utils import FileSystemWork, ReadWriteFile, coroutine, get_world_time

schedule_logger = setup_logger('schedule')
from multiprocessing import Process, Queue


class Scheduler:
    """
    Планировщик задач.
    """
    def __init__(self, pool_size=10, working_time=60):
        self.pool_size: int = pool_size
        self.running_jobs = []
        self.pending_jobs = []
        self.working_time: int = working_time
        self.__folder_path: str = './.scheduler_temp_folder/'

    def create_directory_for_temp_files(self):
        """
        Создает директорию для хранения файлов с информацией о задачах.
        """
        if os.path.exists(self.__folder_path):
            for file in os.listdir(self.__folder_path):
                os.remove(os.path.join(self.__folder_path, file))
        else:
            os.mkdir(self.__folder_path)

    @coroutine
    def schedule(self):
        """
        Метод добавляет в список задач новую, если пул переполнен,
        задача попадает в список отложенных.
        """

        new_job = yield
        job_file = self.create_file_for_the_job(new_job)

        if len(self.running_jobs) == self.pool_size:
            self.pending_jobs.append((new_job, job_file))
            schedule_logger.info('task was added to pending tasks')
        else:
            self.running_jobs.append((new_job, job_file))
            schedule_logger.info('task would be executed recently')
            try:
                result = new_job.run()
            except Exception:
                # здесь уменьшаем количество попыток для задачи
                new_job._tries += 1
        print(result) # позже удалить
        yield result


    def create_file_for_the_job(self, job):
        """
        Создает временный файл с информацией о задаче.
        """
        job._status = 'AWAITING'
        new_json_file = {**vars(job)}
        new_json_file['_task'] = id(job)
        file_name = f'{self.__folder_path}/{self.job_id}.json'
        with open(file_name, 'w') as f:
            json.dump(new_json_file, f)
        return file_name

    def run(self):
        """
        Запуск планировщика задач.
        Для временных файлов создается рабочая директория.
        """
        self.create_directory_for_temp_files()
        while True:
            schedule_logger.info('waiting for task')
            sleep(1)
            self.working_time -= 1
            if not self.working_time:
                break
        self.stop()

    def stop(self):
        """
        Остановка всех выполняющихся задач.
        """
        schedule_logger.warning('Stopping all jobs - there is no time left.')
        sleep(1)
        for job in self.running_jobs:
            job.stop()
        # здесь проверка что таски действительно завершены.
        schedule_logger.info('All jobs stopped. Bye.')

        shutil.rmtree('./.scheduler_temp_folder')
        sys.exit(0)


def create_tasks_for_scheduler(mng):
    """
    Здесь создаем задачи с рандомным временем.
    """
    job1 = Job(get_world_time, kwargs={'user_timezone': 'europe/samara'})
    job2 = Job(get_world_time, kwargs={'user_timezone': 'europe/moscow'})
    job3 = Job(get_world_time, kwargs={'user_timezone': 'europe/london'})


    for job in (job1, job2, job3):
        mng.scheduler.schedule().send(job)
        sleep(random.choice([1, 2, 3, 0.5, 2.5, 1.5]))


if __name__ == '__main__':
    task_scheduler = Scheduler(pool_size=5, working_time=10)
    mng = multiprocessing.Manager()
    mng.scheduler: Scheduler = task_scheduler

    # Запускаем планировщик и создание задач в разных процессах.
    process1 = Process(target=task_scheduler.run)
    process2 = Process(target=create_tasks_for_scheduler, args=[mng])
    process1.start()
    process2.start()
    process1.join()
    process2.join()
