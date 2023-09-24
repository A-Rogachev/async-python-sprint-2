from utils import coroutine, get_world_time, ReadWriteFile, FileSystemWork
from job import Job
from logging_setup import setup_logger

schedule_logger = setup_logger('schedule')


class Scheduler:
    """
    Планировщик задач.
    """
    def __init__(self, pool_size=10):
        self.pool_size: int = pool_size
        # self.running_jobs: list[Job] = []
        # self.pending_jobs: list[Job] = []

    def schedule(self, new_job: Job):
        """
        Метод добавляет в список задач новую, если пул переполнен,
        задача попадает в список отложенных.
        """
        # if len(self.running_jobs) < self.pool_size:
        #     self.running_jobs.append(new_job)
        #     schedule_logger.info('Job scheduled: %s', new_job)
        # else:
        #     self.pending_jobs.append(new_job)
        #     schedule_logger.info('Job added to pending: %s', new_job)

    def run(self):
        """
        Запуск планировщика задач.
        """
        # for job in self.running_jobs:
        #     job.run()

    def restart(self):
        """
        Перезапуск всех выполняющихся задач.
        """
        # for job in self.running_jobs:
        #     job.stop()
        #     job.run()

    def stop(self):
        """
        Остановка всех выполняющихся задач.
        """
        # for job in self.running_jobs:
        #     job.stop()
        # self.running_jobs = []


if __name__ == '__main__':
    scheduler = Scheduler(pool_size=10)
    # scheduler.schedule(Job(get_world_time, kwargs={'user_timezone': 'europe/samara'}))
    # scheduler.schedule(Job(get_world_time, kwargs={'user_timezone': 'europe/moscow'}))
    # scheduler.run()

