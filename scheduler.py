import datetime
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
from utils import (FileSystemWork, ReadWriteFile, coroutine, get_world_time,
                   get_world_time_slowly)

DATETIME_SCHEDULER_FORMAT: str = r'%d.%m.%Y %H:%M'
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


class JobWithDate(NamedTuple):
    """
    Задача в списке отложенных.
    """

    job: Job
    file: str
    start_time: datetime.datetime


class Scheduler:
    """
    Планировщик задач.
    """
    def __init__(self, pool_size=10, working_time=60):
        self.pool_size: int = pool_size
        self.running_jobs: list[CurrentJob] = []
        self.pending_jobs: list[tuple[Job, str]] = []
        self.jobs_with_start_time: list[tuple[JobWithDate, str]] = []
        self.working_time: int = working_time
        self.__folder_path: str = './.scheduler_temp_folder/'

    def create_directory_for_temp_files(self):
        """
        Создает директорию для хранения файлов с информацией о задачах.
        """
        if os.path.exists(self.__folder_path):
            shutil.rmtree(self.__folder_path)
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

            if new_job:
                executing = self.get_current_executing_job()
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
        В случае если у задачи назначено время выполнения, она попадает
        в приоритетную очередь для задач с временем выполнения, если нет - в
        очередь задач на выполнение. В случае если пул задач переполнен,
        задача отправляется в очередь отложенных задач.
        """
        if len(self.running_jobs) == self.pool_size:
            self.pending_jobs.append(CurrentJob(new_job, job_file))
            log.info('task was added to pending tasks')
        elif (
            new_job._start_at
            and datetime.datetime.now() < datetime.datetime.strptime(
                new_job._start_at,
                DATETIME_SCHEDULER_FORMAT,
            )
        ):
            self.jobs_with_start_time.append(
                JobWithDate(new_job, job_file, new_job._start_at)
            )
            log.info('task would be executed at user time')
        else:
            self.running_jobs.append(CurrentJob(new_job, job_file))
            log.info('task would be executed recently')

    def get_current_executing_job(self) -> CurrentJob | JobWithDate:
        """
        Получает текущую задачу для выполнения.
        """
        if self.jobs_with_start_time:
            for job in self.jobs_with_start_time:
                if datetime.datetime.now() >= datetime.datetime.strptime(
                    job.start_time,
                    DATETIME_SCHEDULER_FORMAT,
                ):
                    return self.jobs_with_start_time.pop()
        else:
            return self.running_jobs.pop()

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
    # Обычная функция
    Job1 = Job(
        get_world_time,
        kwargs={'user_timezone': 'europe/samara'},
    )
    # Функция с замедлением, в случае увеличения параметра max_working_time
    # до 3 секунд, не выполняется (TimeoutError)
    Job2 = Job(
        get_world_time_slowly,
        kwargs={'user_timezone': 'europe/moscow'},
        max_working_time=2,
    )
    # Функция с количеством попыток=0, используется чтобы показать,
    # что функции у которых не осталось попыток на выполнение, завершаются.
    Job3 = Job(
        get_world_time,
        kwargs={'user_timezone': 'europe/saratov'},
        # max_tries=0,
    )
    # Стоп-сигнал для планировщика (пример аварийного завершения работы).
    Job4 = Job(
        get_world_time,
        kwargs={'user_timezone': 'europe/london'},
        # start_at='29.09.2023 02:28',
    )
    # Job4 = StopSignal('STOP', scheduler_process)

    for job in (Job1, Job2, Job3, Job4):
        mng.scheduler.schedule().send(job)
        if isinstance(job, StopSignal):
            sleep(6)
        else:
            sleep(random.choice([1, 2, 3, 0.5, 2.5, 1.5]))


if __name__ == '__main__':
    task_scheduler = Scheduler(pool_size=5, working_time=20)
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
    )
    process_scheduler.start()
    process_task_creator.start()
    process_scheduler.join()
    process_task_creator.join()

# Стоп-сигнал вручную останавливает планировщик.
