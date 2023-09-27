import multiprocessing
import os
import random
import shutil
import sys
from time import sleep
import inspect

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
        self.running_jobs: list[Job | None] = []
        self.pending_jobs: list[Job | None ] = []
        self.time_left: int = working_time

    def create_directory_for_temp_files(self):
        """
        Создает директорию для хранения файлов с информацией о задачах.
        """
        folder_path = './.scheduler_temp_folder/'
        if os.path.exists(folder_path):
            for file in os.listdir(folder_path):
                os.remove(os.path.join(folder_path, file))
        else:
            os.mkdir(folder_path)

    def job_is_already_in_queue(self, job) -> bool:
        """
        Проверяет наличие задачи в очереди.
        """
        return job in self.running_jobs

    @coroutine
    def schedule(self, new_job: Job):
        """
        Метод добавляет в список задач новую, если пул переполнен,
        задача попадает в список отложенных.
        """
        if len(self.running_jobs) == self.pool_size:
            self.pending_jobs.append(new_job)
            schedule_logger.info('task was added to pending tasks')
        else:
            self.running_jobs.append(new_job)
            schedule_logger.info('task would be executed recently')
            try:
                result = new_job.run()
            except Exception:
                # здесь уменьшаем количество попыток для задачи
                new_job._tries += 1

        yield result

        # if len(self.running_jobs) < self.pool_size:
        #     self.running_jobs.append(new_job)
        #     schedule_logger.info('Job scheduled: %s', new_job)
        # else:
        #     self.pending_jobs.append(new_job)
        #     schedule_logger.info('Job added to pending: %s', new_job)

    def run(self):
        """
        Запуск планировщика задач.
        """
        self.create_directory_for_temp_files()
        while True:
            schedule_logger.info('waiting for task')
            sleep(1)
            self.time_left -= 1
            if not self.time_left:
                break
        self.stop()

    def restart(self):
        """
        Перезапуск всех выполняющихся задач.
        """
        # for job in self.running_jobs:
        #     job.stop() 
        #     job.run()

    def stop(self):
        """
        Остановка всех выполняющихся задач.
        """
        schedule_logger.info('Stopping all jobs - there is no time left.')
        sleep(1)
        for job in self.running_jobs:
            job.stop()
        schedule_logger.info('All jobs stopped. Bye.')
        shutil.rmtree('./.scheduler_temp_folder')
        sys.exit(0)

def create_tasks_for_scheduler(mng):
    """
    Здесь создаем задачи с рандомным временем.
    """
    job1 = Job(get_world_time, kwargs={'user_timezone': 'europe/samara'})
    job2 = Job(get_world_time, kwargs={'user_timezone': 'europe/moscow'})


    for job in (job1, job2):
        mng.scheduler.schedule(job)
        sleep(random.choice([1, 2, 3, 0.5, 2.5, 1.5]))


if __name__ == '__main__':
    task_scheduler = Scheduler(pool_size=5, working_time=3)
    mng = multiprocessing.Manager()
    mng.scheduler: Scheduler = task_scheduler

    # Запускаем планировщик и создание задач в разных процессах.
    process1 = Process(target=task_scheduler.run)
    process2 = Process(target=create_tasks_for_scheduler, args=[mng])
    process1.start()
    process2.start()
    process1.join()
    process2.join()
