
import os
import json
from datetime import datetime
from enum import Enum
from typing import Any, Callable

from logging_setup import setup_logger
from utils import coroutine

job_logger = setup_logger(__name__)


class JobStatuses(Enum):
    """
    Статусы задач.
    """

    AWAITING = 'awaiting'
    RUNNING = 'running'
    PAUSED = 'paused'
    STOPPED = 'stopped'
    DONE = 'done'

class JobObject:
    NUMBER = 0


class Job:
    """
    Задача, используемая в работе планировщика.
    """

    def __init__(
        self,
        task: Callable[[Any], Any],
        args=None,
        kwargs=None,
        start_at='',
        max_working_time=-1,
        tries=0,
        dependencies=None,
        job_status=JobStatuses.AWAITING,
    ) -> None:
        """
        Инициализация объекта задачи, используемой в работе планировщика.
        """
        self._task = task
        self._args = args or []
        self._kwargs = kwargs or {}
        self._start_at = start_at or None
        self._max_working_time = max_working_time
        self._tries = tries
        self._dependencies = dependencies or []
        self._job_status = job_status
        self._id = JobObject.NUMBER

        JobObject.NUMBER += 1
        self.file_name = self.create_temp_json_file()
        print(self.file_name)


    def create_temp_json_file(self) -> str:
        """
        Создает файл с информацией о задаче.
        """
        file_path = '.scheduler_temp_folder'
        new_json_file = {
            **vars(self)
        }
        new_json_file['_task'] = id(self._task)
        new_json_file['_job_status'] = str(self._job_status)
        file_name = f'{file_path}/{self._id}_task.json'
        with open(file_name, 'w') as f:
            json.dump(new_json_file, f)

        return file_name

    def run(self):
        """
        Запуск задачи.
        """

        if self.job_dependencies_are_done():
            try:
                return self._task(*self._args, **self._kwargs)
            except Exception as job_error:
                job_logger.error(f'Job error: {job_error}')
                return None
        else:
            ...
                # if self.tries > 0:
                #     self.tries -= 1
                #     yield from self.run()
                # else:
                #     self.is_running = False
                #     return None

    def job_dependencies_are_done(self):
        """
        Проверяет, завершены ли задачи-зависимости.
        """
        return True

    def check_job_dependencies(self):
        """
        Проверяет зависимости задачи.
        """
        if not self._dependencies:
            return True
        else:
            if any(
                (
                    job.status for job in self._dependencies
                    if job.status != JobStatuses.DONE
                )
            ):
                job_logger.info(
                    f'Job dependencies for {self._task.__name__} '
                    'are not done yet.'
                )
                return False

            for dependency_task in self._dependencies:
                if dependency_task.start_at > self.start_at:
                    self.start_at = (
                        dependency_task.start_at
                        + datetime.timedelta(seconds=5)
                    )
                job_logger.info(
                    f'The job {self._task.__name__} was delayed for 5 seconds'
                )
        return True




        # # if self.dependencies:
        # #     for dependency in self.dependencies:
        # #         yield from dependency
        # self.is_running = True
        # for i in self.execute():
        #     print(i)
        # job_logger.info('task was executed')




    @coroutine
    def execute(self):
        try:
            gen = self.task(*self.args, **self.kwargs)
            yield from gen
        except Exception as job_error:
            job_logger.error(f'Job error: {job_error}')
            self.is_running = False
            # if self.tries > 0:
            #     self.tries -= 1
            #     yield from self.run()
            # else:
            #     self.is_running = False
            #     return None

    def pause(self):
        """
        Приостановка задачи.
        """
        self.is_paused = True
        # print('Task paused')

    def stop(self):
        """
        Остановка задачи.
        """
        self.is_paused = False
        self.is_running = False
        # print('Task stopped')
