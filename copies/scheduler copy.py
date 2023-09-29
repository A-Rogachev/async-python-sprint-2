import datetime
import json
import multiprocessing
import os
import shutil
import sys
from time import sleep
from typing import NamedTuple

from job import Job
from logging_setup import setup_logger
from utils import coroutine


DEPENDENCIES_SECONDS_DELAY: int =  10
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
        self.pending_jobs: list[CurrentJob] = []
        self.jobs_with_start_time: list[JobWithDate] = []
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
            self.pending_jobs.insert(0, CurrentJob(new_job, job_file))
            log.info('task was added to pending tasks')
        # случай, когда у задачи есть зависимости
        elif new_job._dependencies:
            date_max, job_with_date_max = datetime.datetime.now(), None
            for job in new_job._dependencies:
                if job._start_at and datetime.datetime.strptime(
                    job._start_at,
                    DATETIME_SCHEDULER_FORMAT,
                ) > date_max:
                    date_max = datetime.datetime.strptime(
                        job._start_at,
                        DATETIME_SCHEDULER_FORMAT,
                    )
                    job_with_date_max = job
            if job_with_date_max:
                if job_with_date_max._max_working_time > 0:
                    new_job._start_at = date_max + datetime.timedelta(
                        seconds=job_with_date_max._max_working_time
                    ) + datetime.timedelta(seconds=DEPENDENCIES_SECONDS_DELAY)
                else:
                    new_job._start_at = date_max + datetime.timedelta(
                        seconds=DEPENDENCIES_SECONDS_DELAY
                    )
                self.jobs_with_start_time.insert(
                    0,
                    JobWithDate(
                        new_job,
                        job_file,
                        new_job._start_at,
                    )
                )
            else:
                self.pending_jobs.insert(0, CurrentJob(new_job, job_file))
        elif (
            new_job._start_at
            and datetime.datetime.now() < datetime.datetime.strptime(
                new_job._start_at,
                DATETIME_SCHEDULER_FORMAT,
            )
        ):
            self.jobs_with_start_time.insert(
                0,
                JobWithDate(new_job, job_file, new_job._start_at),
            )
            log.info('task would be executed at user time')
        else:
            self.running_jobs.insert(0, CurrentJob(new_job, job_file))
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
            next_executing_job: CurrentJob = self.running_jobs.pop()
            if self.pending_jobs:
                self.running_jobs.insert(0, self.pending_jobs.pop())
            return next_executing_job


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
        # меняем статусы задач на 'STOPPED' или 'ABORTED'
        # for job in self.running_jobs:
        #     job.stop()
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


if __name__ == '__main__':
    print('Реализация планировщика задач.')