import json
import multiprocessing
import os
import random
import shutil
import sys
from multiprocessing import Process
from time import sleep
from typing import NamedTuple

from job import Job
from logging_setup import setup_logger
from utils import FileSystemWork, ReadWriteFile, coroutine, get_world_time, get_world_time_slowly

log = setup_logger('schedule')


class StopSignal(NamedTuple):
    """
    Сигнал остановки для планировщика.
    """

    signal: str
    process: multiprocessing.Process


class CurrentJob(NamedTuple):
    """
    Задача в процессе выполнения.
    """

    job: Job
    file: str


class Scheduler:
    """
    Планировщик задач.
    """
    def __init__(self, pool_size=10, working_time=60):
        self.pool_size: int = pool_size
        self.running_jobs: list[tuple[Job, str]] = []
        self.pending_jobs: list[tuple[Job, str]] = []
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
        new_job: Job | StopSignal = yield

        if isinstance(new_job, StopSignal):
            self.emergency_exit(new_job.process)
        else:
            job_file = self.create_file_for_the_job(new_job)
            self.put_job_in_the_queue(new_job, job_file)

            executing: CurrentJob = self.running_jobs[-1]

            self.change_job_status(executing.file, 'RUNNING')
            current_try = 1
            while current_try <= executing.job._max_tries:
                try:
                    result = executing.job.run()
                    self.change_job_status(executing.file, 'COMPLETED')
                except Exception:
                    current_try += 1
                else:
                    yield result
            self.change_job_status(executing.file, 'FAILED')
            log.error('task failed: {}'.format(executing.job))
            yield None

    def put_job_in_the_queue(self, new_job, job_file) -> None:
        """
        Проверяет очередь для выполняемых задач.
        """
        if len(self.running_jobs) == self.pool_size:
            self.pending_jobs.append(CurrentJob(new_job, job_file))
            log.info('task was added to pending tasks')
        else:
            self.running_jobs.append(CurrentJob(new_job, job_file))
            log.info('task would be executed recently')

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

    def emergency_exit(self, process):
        """
        Аварийное завершение работы планировщика.
        """
        process.terminate()
        sleep(0.5)
        # shutil.rmtree(self.__folder_path)
        log.error('Scheduler was stopped manually')
        sys.exit(1)


def create_tasks_for_scheduler(
    mng: multiprocessing.Manager,
    scheduler_process: multiprocessing.Process
) -> None:
    """
    Здесь создаем задачи с рандомным временем.
    """
    Job1 = Job(
        get_world_time,
        kwargs={'user_timezone': 'europe/samara'},
    )
    Job2 = Job(
        get_world_time_slowly,
        kwargs={'user_timezone': 'europe/moscow'},
    )
    Job3 = Job(
        get_world_time,
        kwargs={'user_timezone': 'europe/london'},
        max_tries=0,
    )
    Job4 = StopSignal('STOP', scheduler_process)

    for job in (Job1, Job2, Job3, Job4):
        mng.scheduler.schedule().send(job)
        if isinstance(job, StopSignal):
            sleep(6)
        else:
            sleep(random.choice([1, 2, 3, 0.5, 2.5, 1.5]))


if __name__ == '__main__':
    task_scheduler = Scheduler(pool_size=5, working_time=10)
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

# Стоп-сигнал вручную останавливает планировщик.
