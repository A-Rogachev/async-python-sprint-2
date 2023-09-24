import json
import os
import urllib.request
from datetime import datetime
from http import HTTPStatus
from http.client import HTTPResponse
from typing import Any


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
            raise urllib.error.HTTPError
    except urllib.error.URLError and urllib.error.HTTPError:
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


if __name__ == '__main__':
    if not os.path.exists('test_directory'):
        os.mkdir('test_directory')

    # print(get_world_time('europe/samara'))
    # print(get_world_time('europe/moscow'))
    # print(get_world_time('europe/london'))

    ReadWriteFile.rewrite_file('test_directory/test.txt', '123\n456\n789')
    print(ReadWriteFile.read_from_file('test_directory/test.txt'))
    ReadWriteFile.rewrite_file('test_directory/test.txt', 'test')
    print(ReadWriteFile.read_from_file('test_directory/test.txt'))
    ReadWriteFile.add_to_file('test_directory/test2.txt', 'anothertest')
    print(ReadWriteFile.read_from_file('test_directory/test2.txt'))
