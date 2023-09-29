import multiprocessing
import random
from time import sleep

from job import Job
from scheduler import Scheduler, StopSignal
from utils import get_world_time, get_world_time_slowly


def create_tasks_for_scheduler(
    mng: multiprocessing.Manager,
    scheduler_process: multiprocessing.Process
) -> None:
    """
    Здесь создаем задачи с рандомным временем.
    """
    
    # Job1 = Job(
    #     get_world_time,
    #     kwargs={'user_timezone': 'europe/samara'},
    # )
    # # Функция с замедлением, в случае увеличения параметра max_working_time
    # # до 3 секунд, не выполняется (TimeoutError)
    # Job2 = Job(
    #     get_world_time_slowly,
    #     kwargs={'user_timezone': 'europe/moscow'},
    #     max_working_time=1,
    # )
    # # Функция с количеством попыток=0, используется чтобы показать,
    # # что функции у которых не осталось попыток на выполнение, завершаются.
    # Job3 = Job(
    #     get_world_time,
    #     kwargs={'user_timezone': 'europe/saratov'},
    #     max_tries=0,
    # )
    # # Стоп-сигнал для планировщика (пример аварийного завершения работы).
    # Job4 = Job(
    #     get_world_time_slowly,
    #     kwargs={'user_timezone': 'europe/london'},
    #     # start_at='29.09.2023 02:28',
    # )
    # Job5 = Job(
    #     get_world_time_slowly,
    #     kwargs={'user_timezone': 'europe/moscow'},
    #     # start_at='29.09.2023 02:28',
    # )
    # Job6 = Job(
    #     get_world_time_slowly,
    #     kwargs={'user_timezone': 'europe/new_york'},
    #     # start_at='29.09.2023 02:28',
    # )
    # Job7 = StopSignal('STOP', scheduler_process)

    # for job in (Job1, Job2, Job3, Job4, Job5, Job6, Job7):
    #     mng.scheduler.schedule().send(job)
    #     if isinstance(job, StopSignal):
    #         sleep(6)
    #     else:
    #         sleep(random.choice([1, 2, 3, 0.5, 2.5, 1.5]))


if __name__ == '__main__':

    #####################################################
    ##### Для запуска планировщика: python3 main.py #####
    ##### Протестировано на python 3.10.11          #####
    ##### Использованы только встроенные библиотеки #####
    #####################################################

    task_scheduler = Scheduler(pool_size=3, working_time=20)
    mng = multiprocessing.Manager()
    mng.scheduler: Scheduler = task_scheduler

    process_scheduler = multiprocessing.Process(
        name='<SCHEDULER PROCESS>',
        target=task_scheduler.run,
    )
    process_task_creator = multiprocessing.Process(
        name='<TASKS_PROCESS>',
        target=create_tasks_for_scheduler,
        args=[mng, process_scheduler],
    )
    process_scheduler.start()
    process_task_creator.start()
    process_scheduler.join()
    process_task_creator.join()

# Стоп-сигнал вручную останавливает планировщик.
