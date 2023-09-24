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
        self.running_jobs: list[Job] = []
        self.pending_jobs: list[Job] = []

    def schedule(self, new_job: Job):
        """
        Метод добавляет в список задач новую, если пул переполнен,
        задача попадает в список отложенных.
        """
        if len(self.running_jobs) < self.pool_size:
            self.running_jobs.append(new_job)
            schedule_logger.info('Job scheduled: %s', new_job)
        else:
            self.pending_jobs.append(new_job)
            schedule_logger.info('Job added to pending: %s', new_job)

    def run(self):
        for job in self.running_jobs:
            job.run()

    def restart(self):
        """
        Перезапуск всех выполняющихся задач.
        """
        for job in self.running_jobs:
            job.stop()
            job.run()

    def stop(self):
        """
        Остановка всех выполняющихся задач.
        """
        for job in self.running_jobs:
            job.stop()
        self.running_jobs = []


if __name__ == '__main__':
    scheduler = Scheduler(pool_size=3)
    scheduler.schedule(Job(get_world_time, args=['europe/samara']))
    scheduler.run()



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


import json
import os
import shutil
import urllib.request
from datetime import datetime
from functools import wraps
from http import HTTPStatus
from http.client import HTTPResponse
from typing import Any, Callable

# TODO: заменить return в случае ошибок на raise

def coroutine(f: Callable) -> Callable:
    """
    Декоратор для инициализации генератора.
    """
    @wraps(f)
    def wrap(*args, **kwargs):
        gen = f(*args, **kwargs)
        gen.send(None)
        return gen
    return wrap

def get_world_time(user_timezone: str) -> dict[str, Any] | None:
    """
    Использует сервис worldtimeapi.org для получения информации о
    времени и часовом поясе пользователя, исходя из переданных
    пользователем данных.
    """
    service_url: str = (
        'http://worldtimeapi.org/api/timezone/{}'.format(user_timezone)
    )
    try:
        response: HTTPResponse = urllib.request.urlopen(service_url)
        if response.status != HTTPStatus.OK:
            raise urllib.error.HTTPError(
                code=response.status,
                url=service_url,
                msg=response.reason,
                hdrs=response.info(),
                fp=None,
            )
    except (urllib.error.URLError, urllib.error.HTTPError):
        return {
            'error': (
                'can\'t connect to server. '
                'Check connection and your timezone name, then try again'
            )
        }
    data: dict[str, Any] = json.loads(response.read())
    user_datetime: datetime = datetime.strptime(
        data.get('datetime'),
        '%Y-%m-%dT%H:%M:%S.%f%z',
    )

    return {
        'ip address': data.get('client_ip'),
        'date': user_datetime.date().strftime('%d.%m.%Y'),
        'time': user_datetime.time().strftime('%H:%M'),
        'utc_offset': data.get('utc_offset'),
        'utc_datetime': data.get('utc_datetime'),
    }


class ReadWriteFile:
    """
    Класс с задачами, основанными на чтении и записи файлов.
    """

    @staticmethod
    def rewrite_file(filename: str, text: str) -> None:
        """
        Перезаписывает файл с заданным именем.
        В случае, если файл не существует, создает его.
        """
        with open(filename, 'w') as file:
            file.write(text)
        return None

    @staticmethod
    def read_from_file(filename: str) -> str:
        """
        Читает информацию из файла.
        """
        try:
            with open(filename, 'r') as file:
                return file.read()
        except FileNotFoundError:
            return 'File "{}" not found.'.format(filename)

    @staticmethod
    def add_to_file(filename: str, text: str) -> None:
        """
        Добавляет в конец файла переданную информацию.
        В случае если файл не существует, создает его.
        """
        with open(filename, 'a') as file:
            file.write(text)
        return None


class FileSystemWork:
    """
    Методы для взаимодействия с файловой системой.
    """

    @staticmethod
    def remove_object(path: str) -> str | None:
        """
        Удаляет файл/директорию с заданным именем.
        В случае если директория содержит поддиректории и файлы, удаляет и их.
        """
        if os.path.exists(path):
            if os.path.isdir(path):
                shutil.rmtree(path)
            else:
                os.remove(path)
            return None
        return 'Object "{}" not found.'.format(path)

    @staticmethod
    def create_object(path: str, is_folder: bool) -> str | None:
        """
        Создает директорию с заданным именем.
        """
        if os.path.exists(path):
            return 'Object with this name already exists.'
        if is_folder:
            os.makedirs(path)
        else:
            try:
                file = open(path, 'w')
                file.close()
            except OSError:
                return 'Error creating file.'
        return None

    @staticmethod
    def rename_obj(path: str, new_name: str) -> str | None:
        """
        Переименовывает файл/директорию с заданным именем.
        """
        if os.path.exists(path):
            os.rename(path, new_name)
            return None
        return 'Object "{}" not found.'.format(path)


if __name__ == '__main__':
    print('main')
    # if not os.path.exists('test_directory'):
    #     os.mkdir('test_directory')
    # print(FileSystemWork.create_object('test_directory/newfile/234', False))

    # print(get_world_time('europe/samara'))
    # print(get_world_time('europe/moscow'))
    # print(get_world_time('europe/london'))

    # ReadWriteFile.rewrite_file('test_directory/test.txt', '123\n456\n789')
    # print(ReadWriteFile.read_from_file('test_directory/test.txt'))
    # ReadWriteFile.rewrite_file('test_directory/test.txt', 'test')
    # print(ReadWriteFile.read_from_file('test_directory/test.txt'))
    # ReadWriteFile.add_to_file('test_directory/test2.txt', 'anothertest')
    # print(ReadWriteFile.read_from_file('test_directory/test2.txt'))
