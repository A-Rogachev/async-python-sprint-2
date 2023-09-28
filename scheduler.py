import inspect
import multiprocessing
import os
import json
import random
import shutil
import sys
from time import sleep

from typing import NamedTuple
from job import Job
from logging_setup import setup_logger
from utils import FileSystemWork, ReadWriteFile, coroutine, get_world_time

log = setup_logger('schedule')
from multiprocessing import Process, Queue


class StopSignal(NamedTuple):
    """
    Сигнал остановки для планировщика.
    """
    signal: str
    process: multiprocessing.Process


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

    def emergency_exit(self, process):
        """
        Аварийное завершение планировщика.
        """
        process.terminate()
        sleep(0.5)
        shutil.rmtree(self.__folder_path)
        log.error('Scheduler was stopped manually')
        sys.exit(1)

    @coroutine
    def schedule(self):
        """
        Метод добавляет в список задач новую, если пул переполнен,
        задача попадает в список отложенных.
        """
        new_job = yield

        if isinstance(new_job, StopSignal):
            self.emergency_exit(new_job.process)
        else:
            job_file = self.create_file_for_the_job(new_job)

            if len(self.running_jobs) == self.pool_size:
                self.pending_jobs.append((new_job, job_file))
                log.info('task was added to pending tasks')
            else:
                self.running_jobs.append((new_job, job_file))
                log.info('task would be executed recently')

                self.change_job_status(job_file, 'RUNNING')
                while new_job._tries > 0:
                    try:
                        result = new_job.run()
                        self.change_job_status(job_file, 'COMPLETED')
                    except Exception:                 
                        new_job._tries -= 1
                    else:
                        print(result) # позже удалить
                        yield result
                self.change_job_status(job_file, 'FAILED')

    def create_file_for_the_job(self, job):
        """
        Создает временный файл с информацией о задаче.
        """
        job._status = 'AWAITING'
        new_json_file = {**vars(job)}
        new_json_file['_task'] = id(job)
        file_name = f'{self.__folder_path}{id(job)}.json'
        with open(file_name, 'w') as f:
            json.dump(new_json_file, f)
        return file_name

    def change_job_status(self, json_file, status):
        """
        Изменяет статус задачи в файле.
        """
        try:
            with open(json_file, 'r') as f:
                data = json.load(f)
            data['_status'] = status
            with open(json_file, 'w') as f:
                json.dump(data, f)
        except Exception:
            log.warning('scheduler inner error')


    def run(self):
        """
        Запуск планировщика задач.
        Для временных файлов создается рабочая директория.
        """
        self.create_directory_for_temp_files()
        while True:
            log.info('waiting for task')
            sleep(1)
            self.working_time -= 1
            if not self.working_time:
                break
        self.stop()

    def stop(self):
        """
        Остановка всех выполняющихся задач.
        """
        log.warning('Stopping all jobs - there is no time left.')
        sleep(1)
        for job in self.running_jobs:
            job.stop()
        # здесь проверка что таски действительно завершены.
        log.info('All jobs stopped. Bye.')

        shutil.rmtree(self.__folder_path)
        sys.exit(0)

def create_tasks_for_scheduler(mng, scheduler_process):
    """
    Здесь создаем задачи с рандомным временем.
    """
    tasks = [
        Job(
            get_world_time,
            kwargs={'user_timezone': 'europe/samara'},
        ),
        Job(
            get_world_time,
            kwargs={'user_timezone': 'europe/moscow'},
        ),
        Job(
            get_world_time,
            kwargs={'user_timezone': 'europe/london'},
            tries=0,
        ),
        # StopSignal('STOP', scheduler_process),
    ]

    for job in tasks:
        mng.scheduler.schedule().send(job)
        sleep(random.choice([1, 2, 3, 0.5, 2.5, 1.5]))


if __name__ == '__main__':
    task_scheduler = Scheduler(pool_size=5, working_time=30)
    mng = multiprocessing.Manager()
    mng.scheduler: Scheduler = task_scheduler

    process_scheduler = Process(
        name='<SCHEDULER PROCESS>',
        target=task_scheduler.run,
    )
    process_task_creator = Process(
        name='<TASKS_PROCESS>',
        target=create_tasks_for_scheduler,
        args=[mng, process_scheduler],
        daemon=True,
    )
    process_scheduler.start()
    process_task_creator.start()
    process_scheduler.join()
    process_task_creator.join()
