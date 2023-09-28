
import multiprocessing
import os
from datetime import datetime
from typing import Any, Callable

from logging_setup import setup_logger
from utils import coroutine

log = setup_logger(__name__)


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
        max_tries=3,
        dependencies=None,
        job_status='CREATED'
    ) -> None:
        """
        Инициализация объекта задачи, используемой в работе планировщика.
        """
        self._task = task
        self._args = args or []
        self._kwargs = kwargs or {}
        self._start_at = start_at or None
        self._max_working_time = max_working_time
        self._max_tries = max_tries
        self._dependencies = dependencies or []
        self._status = job_status

    def execute_with_timeout(self, timeout: int):
        """
        Выполнение задачи с учетом заданного максимального
        времени выполнения.
        """

        task_process = multiprocessing.Process(
            target=self._task,
            args=self._args,
            kwargs=self._kwargs,
        )
        task_process.start()
        task_process.join(timeout)
        if task_process.is_alive():
            task_process.terminate()
            task_process.join()
            raise TimeoutError('Task execution timed out')

    def run(self):
        """
        Запуск задачи.
        """
        try:
            if self._max_working_time > 0:
                return self.execute_with_timeout(self._max_working_time)
            return self._task(*self._args, **self._kwargs)
        except Exception as job_error:
            log.error(f'Job error: {job_error}')
            raise Exception('Job error')

    def __str__(self):
        """
        Строковое представление задачи.
        """
        return f'<Job {self._task.__name__}, {id(self)}>'
