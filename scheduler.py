import datetime
import logging
import multiprocessing
import os
import queue
import shutil
import sys
from threading import Lock
from time import sleep
from typing import NamedTuple

from job import Job
from logging_setup import setup_logger
from utils import coroutine

# DEPENDENCIES_SECONDS_DELAY: int =  10

log: logging.Logger = setup_logger('schedule')
right_now = datetime.datetime.now
lock = Lock()

emergency_event = multiprocessing.Event()

class StopSignal(NamedTuple):
    """
    Сигнал остановки для планировщика.
    """

    signal: str
    process: multiprocessing.Process


class Scheduler:
    """
    Инициализация объекта класса Scheduler (планировщик задач).
    """
    def __init__(self, pool_size=10, working_time=60):
        self.pool_size: int = pool_size
        self.running_jobs = multiprocessing.Manager().list()
        self.pending_jobs = multiprocessing.Manager().list()
        self.delayed_jobs = multiprocessing.Manager().list()
        self.working_time: int = working_time
        self.__folder_path: str = './.scheduler_temp_folder/'

    def run(self):
        """
        Запуск планировщика задач.
        """
        self._create_directory_for_temp_files()
        self._run_daemon_services()
        while True:
            log.info('waiting for task')
            sleep(1)
            self.working_time -= 1
            if not self.working_time:
                break
        self._stop()

    @coroutine
    def schedule(self):
        """
        Метод добавляет в список задач новую, если пул переполнен,
        задача попадает в список отложенных.
        """
        new_job: Job | StopSignal = yield

        if isinstance(new_job, StopSignal):
            self._emergency_exit(new_job.process)

        if new_job._start_at and right_now() < new_job._start_at:
            lock.acquire()           
            self.delayed_jobs.insert(0, new_job)
            lock.release()
            log.warning(
                f'Task id{id(new_job)} will be executed at '
                f'{new_job._start_at.strftime("%H:%M:%S")}'
            )
        else:
            log.info(f'Task id{id(new_job)} will be executed right now.')
        yield self.execute().send(new_job)

    @coroutine
    def execute(self):
        """
        Выполнение задачи.
        """
        current_task: Job | StopSignal = yield
        current_try: int = 1
        while current_try <= current_task._max_tries:
            try:
                result = current_task.run()
            except TimeoutError:
                log.error(f'Task {current_task} - failed (timeout error).')
                break
            except Exception:
                log.error(f'Task {current_task} - try {current_try} failed (available tries: {current_task._max_tries}).')
                if current_try + 1 > current_task._max_tries:
                    break
            else:
                log.success(f'Task finished: {current_task}')
                yield result
        yield None

    def _check_delayed_jobs(self, delayed_jobs):
            """
            Проверка задач с отложенным запуском (с указанным временем запуска).
            """
            while True:
                if emergency_event.is_set():
                    sys.exit(0)
                sleep(0.5)
                if delayed_jobs:
                    new_task_is_ready: bool = False
                    for i, job in enumerate(delayed_jobs):
                        if right_now() > job._start_at:
                            new_task_is_ready = True
                            break
                    if new_task_is_ready:
                        lock.acquire()
                        next_job = delayed_jobs.pop(i)
                        lock.release()
                        next_job._start_at = None
                        self.schedule().send(next_job)

    def _check_pending_jobs(self, running_jobs, pending_jobs):
        """
        Проверка очереди выполняемых задач.
        """
        while True:
            sleep(0.5)
            print(running_jobs, pending_jobs)

###############################################################################
    def _create_directory_for_temp_files(self) -> None:
        """
        Создает директорию для хранения файлов с информацией о задачах.
        """
        if os.path.exists(self.__folder_path):
            shutil.rmtree(self.__folder_path)
        os.mkdir(self.__folder_path)

    def _run_daemon_services(self) -> None:
        """
        Запускает службы, отслеживающие очереди задач.
        """
        checking_delayed_process = multiprocessing.Process(
            target=self._check_delayed_jobs,
            args=[self.delayed_jobs],
            daemon=True,
        )
        checking_pending_process = multiprocessing.Process(
            target=self._check_pending_jobs,
            args=[self.pending_jobs, self.pending_jobs],
            daemon=True,
        )
        checking_delayed_process.start()
        checking_pending_process.start()

    def _stop(self):
        """
        Завершение работы планировщика.
        """
        log.warning('Stopping all jobs - there is no time left.')
        sleep(1)
        log.info('All jobs stopped. Bye.')
        sys.exit(0)

    def _emergency_exit(self, process):
        """
        Аварийное завершение работы планировщика.
        """
        emergency_event.set()
        process.terminate()
        sleep(0.5)
        shutil.rmtree(self.__folder_path)
        log.warning('Scheduler was stopped manually')
        sys.exit(0)

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






