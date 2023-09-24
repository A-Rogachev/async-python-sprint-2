import urllib.request
from http.client import HTTPResponse
from typing import Any
import json
from datetime import datetime


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
        if response.status != 200:
            raise urllib.error.HTTPError
    except urllib.error.URLError and urllib.error.HTTPError:
        return {
            'error': (
                'can\'t connect to server. Check connection and your timezone name, then try again'
            )
        }
    data: dict[str, Any] = json.loads(response.read())
    user_datetime = datetime.strptime(data.get('datetime'), '%Y-%m-%dT%H:%M:%S.%f%z')

    return {
        'ip address': data.get('client_ip'),
        'date': user_datetime.date().strftime('%d.%m.%Y'),
        'time': user_datetime.time().strftime('%H:%M'),
        'utc_offset': data.get('utc_offset'),
        'utc_datetime': data.get('utc_datetime'),
    }

if __name__ == '__main__':
    print(get_world_time('europe/samara'))