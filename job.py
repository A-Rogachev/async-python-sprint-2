
import datetime
import multiprocessing
from enum import Enum
from typing import Any, Callable

from logging_setup import setup_logger

log = setup_logger(__name__)


class JOB_STATUSES(Enum):
    """
    Возможные статусы задачи.
    """

    CREATED = 'CREATED'
    DELAYED = 'DELAYED'
    IS_PENDED = 'IS_PENDED'
    READY_TO_RUN = 'READY_TO_RUN'
    COMPLETED = 'COMPLETED'
    FAILED = 'FAILED'


class Job:
    """
    Задача, используемая в работе планировщика.
    """

    def __init__(
        self,
        name: str | None,
        task: Callable[[Any], Any],
        args: Any | None = None,
        kwargs: dict[str, Any] | None = None,
        start_at: datetime.datetime | None = None,
        max_working_time: int | None = None,
        max_tries: int = 3,
        dependencies: list[str] | None = None,
        job_status: str = 'CREATED'
    ) -> None:
        """
        Инициализация объекта задачи, используемой в работе планировщика.
        """
        self._name = name or ''
        self._task = task
        self._args = args or []
        self._kwargs = kwargs or {}
        self._start_at = start_at or None
        self._max_working_time = max_working_time
        self._max_tries = max_tries

        self._dependencies = dependencies or []
        self._status = job_status

    def run(self):
        """
        Запуск задачи.
        """
        if self._max_working_time:
            return self.execute_with_timeout(self._max_working_time)
        return self._task(*self._args, **self._kwargs)

    def execute_with_timeout(self, timeout: int) -> Any:
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

    def __str__(self):
        """
        Строковое представление задачи.
        """
        return f'<Job {id(self)}>'

    def __repr__(self) -> str:
        """
        Строковое формальное представление задачи.
        """
        return self.__str__()
