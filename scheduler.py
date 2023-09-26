from utils import coroutine, get_world_time, ReadWriteFile, FileSystemWork
from job import Job
from logging_setup import setup_logger
from time import sleep
import os
import sys
import multiprocessing

schedule_logger = setup_logger('schedule')
from multiprocessing import Process, Queue

class Scheduler:
    """
    Планировщик задач.
    """
    def __init__(self, pool_size=10):
        self.pool_size: int = pool_size
        self.running_jobs: list[Job | None] = []
        self.pending_jobs: list[Job | None ] = []
        self.create_directory_for_temp_files()

    check_working_time()

    def create_directory_for_temp_files(self):
        """
        Создает директорию для хранения файлов с информацией о задачах.
        """
        folder_path = './.scheduler_temp_folder/'
        if os.path.exists(folder_path):
            for file in os.listdir(folder_path):
                os.remove(os.path.join(folder_path, file))
        else:
            os.mkdir(folder_path)

    def job_is_already_in_queue(self, job) -> bool:
        """
        Проверяет наличие задачи в очереди.
        """
        return job in self.running_jobs

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

    @coroutine
    def run(self):
        """
        Запуск планировщика задач.
        """
        while True:
            schedule_logger.info('waiting for task')
            sleep(1)
            # new_task = yield
            # print(f'got new task: {new_task}')
            # yield new_task
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


def create_tasks():
    for i in range(10):
        sleep(0.5)
        print(i)



if __name__ == '__main__':
    task_scheduler = Scheduler(pool_size=10)

    

    # jobs = [
    #     Job(get_world_time, kwargs={'user_timezone': 'europe/samara'}),
    #     Job(get_world_time, kwargs={'user_timezone': 'europe/moscow'}),
    # ]
    # for job in jobs:
    #     task_scheduler.schedule(job)

    process1 = Process(target=task_scheduler.run)
    process2 = Process(target=create_tasks)
    process1.start()
    process2.start()
    process1.join()
    process2.join()
    # task_scheduler.run()
