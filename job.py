from .utils import coroutine

class Job:
    """
    Задача, используемая в работе планировщика.
    """

    def __init__(
        self,
        start_at="",
        max_working_time=-1,
        tries=0,
        dependencies=None
    ) -> None:
        """
        Инициализация объекта задачи, используемой в работе планировщика.
        """
        self.start_at = start_at
        self.max_working_time = max_working_time
        self.tries = tries
        self.dependencies = dependencies or []
        pass

    def run(self):
        pass

    def pause(self):
        pass

    def stop(self):
        pass
