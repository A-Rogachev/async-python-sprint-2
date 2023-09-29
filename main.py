import datetime
import multiprocessing
from time import sleep

from job import Job
from scheduler import Scheduler, StopSignal, right_now
from utils import function_with_error, get_world_time, get_world_time_slowly


def user_tasks_for_scheduler(
    mng: multiprocessing.Manager,
    scheduler_process: multiprocessing.Process
) -> None:
    """
    Создание задач для представления работы планировщика.
    """

    Job1 = Job(
        'Обычная задача',
        get_world_time,
        kwargs={'user_timezone': 'europe/samara'},
    )
    Job2 = Job(
        'Запланированная задача - позже на 15 сек.',
        get_world_time,
        kwargs={'user_timezone': 'europe/moscow'},
        start_at=right_now() + datetime.timedelta(seconds=15)
    )
    Job3 = Job(
        'Запланированная задача - позже на 12 сек.',
        get_world_time,
        kwargs={'user_timezone': 'europe/london'},
        start_at=right_now() + datetime.timedelta(seconds=12)
    )
    Job4 = Job(
        'Время попыток уменьшено до 0, с целью показать обработку ошибок',
        function_with_error,
        max_tries=1,
    )
    Job5 = Job(
        'Функция для показателя работы таймаута, переданного пользователем',
        get_world_time_slowly,
        kwargs={'user_timezone': 'europe/rome'},
        max_working_time=2,
    )
    stop_signal = StopSignal('STOP', scheduler_process)

    for job in (Job1, Job2, Job3, Job4, Job5):
        sleep(3)
        mng.scheduler.schedule().send(job)
    sleep(5)
    mng.scheduler.schedule().send(stop_signal)        

#####################################################
##### Для запуска планировщика: python3 main.py #####
##### Протестировано на python 3.10.11          #####
##### Использованы только встроенные библиотеки #####
#####################################################


if __name__ == '__main__':
    task_scheduler = Scheduler(pool_size=3, working_time=40)

    mng = multiprocessing.Manager()
    mng.scheduler: Scheduler = task_scheduler

    process_scheduler = multiprocessing.Process(
        name='<SCHEDULER PROCESS>',
        target=task_scheduler.run,
    )
    process_task_creator = multiprocessing.Process(
        name='<USER_PROCESS>',
        target=user_tasks_for_scheduler,
        args=[mng, process_scheduler],
    )
    process_scheduler.start()
    process_task_creator.start()
    process_scheduler.join()
    process_task_creator.join()

# Стоп-сигнал вручную останавливает планировщик.
