from multiprocessing import Queue

def send_task(queue, task):
    queue.put(task)

if __name__ == "__main__":
    task_queue = Queue()

    while True:
        task = input("Enter a task (or 'exit' to quit): ")
        if task == "exit":
            break
        send_task(task_queue, task)