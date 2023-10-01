import datetime
import json
import os
import shutil
import urllib.request
from functools import wraps
from http import HTTPStatus
from http.client import HTTPResponse
from time import sleep
from typing import Any, Callable


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
    user_datetime: datetime.datetime | None = datetime.datetime.strptime(
        data.get('datetime'),
        '%Y-%m-%dT%H:%M:%S.%f%z',
    )
    result = {
        'ip address': data.get('client_ip'),
        'date': user_datetime.date().strftime('%d.%m.%Y'),
        'time': user_datetime.time().strftime('%H:%M'),
        'utc_offset': data.get('utc_offset'),
        'utc_datetime': data.get('utc_datetime'),
        'time_zone': user_timezone,
    }
    print(result)   # NOTE: побочный эффект выполнения функции.
    return result


def function_with_error():
    """
    Функция для представления обработки количества попыток запуска задачи.
    """
    raise Exception('This was requested error just to show the function works.')
    

def get_world_time_slowly(user_timezone: str) -> dict[str, Any] | None:
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
    user_datetime: datetime.datetime = datetime.datetime.strptime(
        data.get('datetime'),
        '%Y-%m-%dT%H:%M:%S.%f%z',
    )
    ##########################################################################
    sleep(4)
    ##########################################################################
    result = {
        'ip address': data.get('client_ip'),
        'date': user_datetime.date().strftime('%d.%m.%Y'),
        'time': user_datetime.time().strftime('%H:%M'),
        'utc_offset': data.get('utc_offset'),
        'utc_datetime': data.get('utc_datetime'),
        'time_zone': user_timezone,
    }
    print(result)   # NOTE: побочный эффект выполнения функции.
    return result


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


class FileSystemWork:
    """
    Метод для взаимодействия с файловой системой.
    """

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
