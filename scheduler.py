import datetime
import json
import logging
import multiprocessing
import os
import shutil
import sys
from time import sleep
from typing import Any, NamedTuple

from job import JOB_STATUSES, Job
from logging_setup import setup_logger
from utils import coroutine

DEPENDENCIES_SECONDS_DELAY: int = 3

log: logging.Logger = setup_logger('schedule')
right_now = datetime.datetime.now
lock = multiprocessing.Lock()
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
        self.__id_for_tasks: int = 0

    def run(self):
        """
        Запуск планировщика задач.
        """
        self._create_directory_for_temp_files()
        self._run_daemon_services()
        while True:
            log.info('Scheduler is running and ready to get new tasks.')
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

        new_job._id_inside_scheduler = self.__id_for_tasks
        self.__id_for_tasks += 1

        if new_job._dependencies:
            lock.acquire()
            dependency_times: list[datetime.datetime | None] = [
                dependency._start_at
                for dependency
                in new_job._dependencies
                if dependency._start_at
            ]
            max_dependency_start: datetime.datetime = (
                max(
                    dependency_times
                ) + datetime.timedelta(seconds=DEPENDENCIES_SECONDS_DELAY)
            ) if dependency_times else right_now()

            if not new_job._start_at:
                new_job._start_at = max_dependency_start
            elif new_job._start_at < max_dependency_start:
                new_job._start_at = max_dependency_start

            new_job._status = JOB_STATUSES.DELAYED
            self._create_temp_job_file(new_job)
            self.delayed_jobs.insert(0, new_job)
            lock.release()
            log.warning(
                f'Task {new_job} will be executed after its dependencies done.'
            )
            log.warning(f'{new_job._start_at}')
        elif new_job._start_at and right_now() < new_job._start_at:
            lock.acquire()
            new_job._status = JOB_STATUSES.DELAYED
            self._create_temp_job_file(new_job)
            self.delayed_jobs.insert(0, new_job)
            lock.release()
            log.warning(
                f'Task {new_job} will be executed at '
                f'{new_job._start_at.strftime("%H:%M:%S")}'
            )
        else:
            lock.acquire()
            if len(self.running_jobs) + 1 > self.pool_size:
                log.warning(
                    'Task pool is full. Job would be '
                    'executed a bit later.'
                )
                new_job._status = JOB_STATUSES.IS_PENDED
                self._create_temp_job_file(new_job)
                self.pending_jobs.insert(0, new_job)
            else:
                log.info(
                    f'Task {new_job} will be executed soon.'
                )
                new_job._status = JOB_STATUSES.READY_TO_RUN
                self.running_jobs.insert(0, new_job)
            lock.release()
        yield None

    def _check_delayed_jobs(
        self,
        delayed_jobs: multiprocessing.list[Job],
    ) -> None:
        """
        Проверка задач с отложенным запуском (с указанным временем).
        """
        while True:
            if emergency_event.is_set():
                sys.exit(0)
            sleep(0.5)
            if delayed_jobs:
                new_task_is_ready: bool = False
                for i, job in enumerate(delayed_jobs):
                    if right_now() > job._start_at:
                        if job._dependencies:
                            for dependency in job._dependencies:
                                if (
                                    dependency._status
                                    != JOB_STATUSES.COMPLETED
                                ):
                                    break
                        new_task_is_ready = True
                        break
                if new_task_is_ready:
                    lock.acquire()
                    next_job: Job = delayed_jobs[i]
                    lock.release()
                    next_job._status: JOB_STATUSES = JOB_STATUSES.READY_TO_RUN
                    result_of_execution = str(self._execute().send(next_job))
                    lock.acquire()

                    if next_job._status in (
                        JOB_STATUSES.COMPLETED,
                        JOB_STATUSES.FAILED
                    ):
                        delayed_jobs.pop(i)
                    self._change_temp_file(
                        f'{self.__folder_path}task_'
                        f'{next_job._id_inside_scheduler}.json',
                        {
                            '_status': str(next_job._status).removeprefix(
                                'JOB_STATUSES.'
                            ),
                            '_result': result_of_execution,
                        }
                    )
                    lock.release()

    def _check_pending_jobs(
        self,
        running_jobs: multiprocessing.list[Job],
        pending_jobs: multiprocessing.list[Job],
    ) -> None:
        """
        Проверка очереди выполняемых задач.
        """
        while True:
            if emergency_event.is_set():
                sys.exit(0)
            sleep(0.5)
            if len(running_jobs) > 0:
                lock.acquire()
                next_task: Job = running_jobs[-1]
                lock.release()

                json_temp_file: str = self._create_temp_job_file(next_task)
                result_of_execution = str(self._execute().send(next_task))

                lock.acquire()
                if next_task._status in (
                    JOB_STATUSES.COMPLETED,
                    JOB_STATUSES.FAILED
                ):
                    running_jobs.pop()
                    self._change_temp_file(
                        json_temp_file, {
                            '_status': str(next_task._status).removeprefix(
                                'JOB_STATUSES.'
                            ),
                            '_result': result_of_execution,
                        }
                    )
                lock.release()

            elif len(running_jobs) == 0 and len(pending_jobs) > 1:
                lock.acquire()
                running_jobs.insert(0, pending_jobs.pop())
                lock.release()

    @coroutine
    def _execute(self):
        """
        Выполнение задачи.
        """
        current_task: Job | StopSignal = yield
        current_try: int = 1
        while current_try <= current_task._max_tries:
            try:
                result = current_task.run()
            except Exception:
                log.error(
                    f'Task {current_task} - try {current_try} failed '
                    f'(available tries: {current_task._max_tries}).'
                )
                if current_try + 1 > current_task._max_tries:
                    break
                current_try += 1
            else:
                log.success(f'Task finished: {current_task}')
                current_task._status = JOB_STATUSES.COMPLETED
                yield result
        current_task._status = JOB_STATUSES.FAILED
        self._change_temp_file(
            f'{self.__folder_path}task_'
            f'{current_task._id_inside_scheduler}.json',
            {
                '_status': str(current_task._status).removeprefix(
                    'JOB_STATUSES.'
                ),
                '_result': 'ERROR',
            }
        )
        yield None

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
            name='DELAY_JOBS_PROCESS',
            target=self._check_delayed_jobs,
            args=[self.delayed_jobs],
            daemon=True,
        )
        checking_pending_process = multiprocessing.Process(
            name='PENDING_JOBS_PROCESS',
            target=self._check_pending_jobs,
            args=[self.running_jobs, self.pending_jobs],
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
        log.warning('Scheduler was stopped manually')
        sys.exit(0)

    def _create_temp_job_file(self, job: Job) -> str | None:
        """
        Создает временный файл с информацией о задаче.
        """
        try:
            new_json_file = {
                key: str(value) for key, value in vars(job).items()
            }
            new_json_file['_result'] = ''
            file_name: str = (
                f'{self.__folder_path}task_{job._id_inside_scheduler}.json'
            )
            with open(file_name, 'w') as f:
                json.dump(new_json_file, f)
            return file_name
        except Exception:
            log.error('Error creating temp file')
            return None

    def _change_temp_file(self, json_file: str, args=None) -> None:
        """
        Изменяет статус задачи в файле.
        """
        try:
            with open(json_file, 'r') as f:
                data: dict[str, Any] = json.load(f)
            for changing_arg in args:
                data[changing_arg] = args.get(changing_arg)
            with open(json_file, 'w') as f:
                json.dump(data, f)
        except Exception as error:
            log.error('Error changing temp scheduler file', error)
