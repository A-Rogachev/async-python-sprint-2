from datetime import datetime
from logging_setup import setup_logger
from utils import coroutine

job_logger = setup_logger(__name__)

class Job:
    """
    Задача, используемая в работе планировщика.
    """

    def __init__(
        self,
        task,
        args=None,
        kwargs=None,
        start_at="",
        max_working_time=-1,
        tries=0,
        dependencies=None
    ) -> None:
        """
        Инициализация объекта задачи, используемой в работе планировщика.
        """
        self.task = task
        self.args = args or []
        self.kwargs = kwargs or []
        self.start_at = datetime.now()
        self.max_working_time = max_working_time
        self.tries = tries
        self.dependencies = dependencies or []
        self.is_running = False
        self.is_paused = False

    def run(self):
        """
        Запускает задачу.
        """
        # if self.dependencies:
        #     for dependency in self.dependencies:
        #         yield from dependency
        self.is_running = True
        self.execute()

    @coroutine
    def execute(self):
        try:
            return self.task(*self.args, **self.kwargs)
        except Exception as job_error:
            job_logger.error(f'Job error: {job_error}')
            if self.tries > 0:
                self.tries -= 1
                yield from self.run()
            else:
                self.is_running = False
                return None

    def pause(self):
        ...
        # self.is_paused = True
        # print('Task paused')

    def stop(self):
        ...
        # self.is_paused = False
        # self.is_running = False
        # print('Task stopped')
