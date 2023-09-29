import datetime
import json
import multiprocessing
import os
import queue
import shutil
import sys
import threading
from threading import Lock
from time import sleep
from typing import NamedTuple

from job import Job
from logging_setup import setup_logger
from utils import coroutine, function_with_error, get_world_time

# DEPENDENCIES_SECONDS_DELAY: int =  10

log = setup_logger('schedule')
right_now = datetime.datetime.now
lock = Lock()

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
        self.running_jobs = queue.Queue()
        self.pending_jobs = queue.Queue()
        self.delayed_jobs = multiprocessing.Manager().list()
        self.working_time = working_time
        self.__folder_path: str = './.scheduler_temp_folder/'

        self.create_directory_for_temp_files()
        self.checking_process = multiprocessing.Process(target=self.checking_new_jobs, args=[self.delayed_jobs], daemon=True)
        self.checking_process.start()
        

    def run(self):
        """
        Запуск планировщика задач.
        """
        while True:
            log.info('waiting for task')
            sleep(1)
            self.working_time -= 1
            if not self.working_time:
                break
        self.stop()

    def checking_new_jobs(self, delayed_jobs):
        """
        Проверка задач на выполнение.
        """
        while True:
            sleep(1)
            if delayed_jobs:
                new_task_is_ready: bool = False
                for i, job in enumerate(delayed_jobs):
                    if right_now() > job._start_at:
                        new_task_is_ready = True
                        break
                if new_task_is_ready:
                    next_job = delayed_jobs.pop(i)
                    next_job._start_at = None
                    self.schedule().send(next_job)

    @coroutine
    def schedule(self):
        """
        Метод добавляет в список задач новую, если пул переполнен,
        задача попадает в список отложенных.
        """
        new_job = yield

        # В случае если получаем стоп-сигнал, завершаем работу планировщика.
        if isinstance(new_job, StopSignal):
            self.emergency_exit(new_job.process)

        # В случае если новая задача имеет конкретное время выполнения.
        if new_job._start_at and right_now() < new_job._start_at:
            lock.acquire()           
            self.delayed_jobs.insert(0, new_job)
            lock.release()
            log.warning(f'Task id{id(new_job)} will be executed at {new_job._start_at}.')
        else:
        # Случай с обычной задачей.
            log.info(f'Task id{id(new_job)} will be executed right now.')

            current_try: int = 1
            while current_try <= new_job._max_tries:
                try:
                    result = new_job.run()
                except Exception:
                    log.error(f'Task {new_job} - try {current_try} failed (available tries: {new_job._max_tries}).')
                    if current_try + 1 > new_job._max_tries:
                        yield None
                else:
                    log.info(f'Task finished: {new_job}')
                    yield result
        yield None

            # job_file = self.create_file_for_the_job(new_job)
            # self.put_job_in_the_queue(new_job, job_file)
            # if new_job:
            #     executing = self.get_current_executing_job()
            #     self.change_job_status(executing.file, 'RUNNING')
            #     
            #     self.change_job_status(executing.file, 'FAILED')
            #     log.error('task failed: {}'.format(executing.job))
            #     yield None

###############################################################################
    def create_directory_for_temp_files(self):
        """
        Создает директорию для хранения файлов с информацией о задачах.
        """
        if os.path.exists(self.__folder_path):
            shutil.rmtree(self.__folder_path)
        os.mkdir(self.__folder_path)
        
    def stop(self):
        """
        Завершение работы планировщика.
        """
        log.warning('Stopping all jobs - there is no time left.')
        sleep(1)
        log.info('All jobs stopped. Bye.')
        sys.exit(0)

    def emergency_exit(self, process):
        """
        Аварийное завершение работы планировщика.
        """
        process.terminate()
        sleep(0.5)
        shutil.rmtree(self.__folder_path)
        log.error('Scheduler was stopped manually')
        sys.exit(1)

################################################################################        
##############################################################################

#     def put_job_in_the_queue(self, new_job, job_file) -> None:
#         """
#         Проверяет очередь для выполняемых задач.
#         В случае если у задачи назначено время выполнения, она попадает
#         в приоритетную очередь для задач с временем выполнения, если нет - в
#         очередь задач на выполнение. В случае если пул задач переполнен,
#         задача отправляется в очередь отложенных задач.
#         """
#         if len(self.running_jobs) == self.pool_size:
#             self.pending_jobs.insert(0, CurrentJob(new_job, job_file))
#             log.info('task was added to pending tasks')
#         # случай, когда у задачи есть зависимости
#         elif new_job._dependencies:
#             date_max, job_with_date_max = datetime.datetime.now(), None
#             for job in new_job._dependencies:
#                 if job._start_at and datetime.datetime.strptime(
#                     job._start_at,
#                     DATETIME_SCHEDULER_FORMAT,
#                 ) > date_max:
#                     date_max = datetime.datetime.strptime(
#                         job._start_at,
#                         DATETIME_SCHEDULER_FORMAT,
#                     )
#                     job_with_date_max = job
#             if job_with_date_max:
#                 if job_with_date_max._max_working_time > 0:
#                     new_job._start_at = date_max + datetime.timedelta(
#                         seconds=job_with_date_max._max_working_time
#                     ) + datetime.timedelta(seconds=DEPENDENCIES_SECONDS_DELAY)
#                 else:
#                     new_job._start_at = date_max + datetime.timedelta(
#                         seconds=DEPENDENCIES_SECONDS_DELAY
#                     )
#                 self.jobs_with_start_time.insert(
#                     0,
#                     JobWithDate(
#                         new_job,
#                         job_file,
#                         new_job._start_at,
#                     )
#                 )
#             else:
#                 self.pending_jobs.insert(0, CurrentJob(new_job, job_file))
#         elif (
#             new_job._start_at
#             and datetime.datetime.now() < datetime.datetime.strptime(
#                 new_job._start_at,
#                 DATETIME_SCHEDULER_FORMAT,
#             )
#         ):
#             self.jobs_with_start_time.insert(
#                 0,
#                 JobWithDate(new_job, job_file, new_job._start_at),
#             )
#             log.info('task would be executed at user time')
#         else:
#             self.running_jobs.insert(0, CurrentJob(new_job, job_file))
#             log.info('task would be executed recently')

#     def get_current_executing_job(self) -> CurrentJob | JobWithDate:
#         """
#         Получает текущую задачу для выполнения.
#         """
#         if self.jobs_with_start_time:
#             for job in self.jobs_with_start_time:
#                 if datetime.datetime.now() >= datetime.datetime.strptime(
#                     job.start_time,
#                     DATETIME_SCHEDULER_FORMAT,
#                 ):
#                     return self.jobs_with_start_time.pop()
#         else:
#             next_executing_job: CurrentJob = self.running_jobs.pop()
#             if self.pending_jobs:
#                 self.running_jobs.insert(0, self.pending_jobs.pop())
#             return next_executing_job


#     def create_file_for_the_job(self, job):
#         """
#         Создает временный файл с информацией о задаче.
#         """
#         job._status = 'AWAITING'
#         new_json_file = {**vars(job)}
#         new_json_file['_task'] = id(job)
#         file_name = f'{self.__folder_path}{id(job)}.json'
#         with open(file_name, 'w') as f:
#             json.dump(new_json_file, f)
#         return file_name

#     def change_job_status(self, json_file, status):
#         """
#         Изменяет статус задачи в файле.
#         """
#         try:
#             with open(json_file, 'r') as f:
#                 data = json.load(f)
#             data['_status'] = status
#             with open(json_file, 'w') as f:
#                 json.dump(data, f)
#         except Exception:
#             log.warning('scheduler inner error')






