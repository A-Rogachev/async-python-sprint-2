from functools import wraps
from queue import Queue
from typing import Any, Generator, Callable

def coroutine(f):
    @wraps(f)
    def wrap(*args, **kwargs):
        gen = f(*args, **kwargs)
        gen.send(None)
        return gen

    return wrap

class Job:
    def __init__(self, target: Callable, args: tuple = None, kwargs: dict = None) -> None:
        self.__args = args or ()
        self.__kwargs = kwargs or {}
        self.__coroutine = target(*self.__args, **self.__kwargs)

    def run(self) -> None:
        self.__coroutine.send(None)

class JobLoop:
    def __init__(self) -> None:
        self.__queue: Queue[Job] = Queue()

    def add_job(self, task: Job) -> None:
        self.__queue.put(task)

    def run(self) -> None:
        while not self.__queue.empty():
            job = self.__queue.get()
            try:
                job.run()
            except StopIteration:
                continue
            self.add_job(job)

@coroutine
def print_x(num: int, name: str) -> Generator[None, None, None]:
    for i in range(num):
        yield
        print(f'Вызов функции print_x с именем {name}')

if __name__ == '__main__':
    job1 = Job(target=print_x, args=(2, 'Два'))
    job2 = Job(target=print_x, args=(3, 'Три'))
    job3 = Job(target=print_x, args=(4, 'Четыре'))

    loop = JobLoop()
    loop.add_job(job1)
    loop.add_job(job2)
    loop.add_job(job3)
    loop.run()
