from undine.utils.system import System
from threading import Semaphore, Thread
from multiprocessing import Queue

import subprocess

import threading


class TaskThread:
    def __init__(self):
        self._state = None
        self._result = 'Empty'
        self._error = 'Empty'

    def run(self, cmd):
        process = subprocess.Popen(cmd,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE,
                                   shell=True)

        self._result, self._error = process.communicate()

        self._state = process.returncode

    @property
    def result_message(self):
        return bytes(self._result).decode('utf-8')

    @property
    def error_message(self):
        return bytes(self._error).decode('utf-8')

    @property
    def state(self):
        return self._state


class TaskScheduler:
    def __init__(self, manager, config):
        system_cpu = System.cpu_cores() - 1
        config_cpu = int(config.setdefault('max_cpu', '0'))

        self._workers = max(system_cpu, config_cpu)
        self._manager = manager
        self._pool = Semaphore(self._workers)

        # TODO Check thread table is useful.
        self._ticket = Queue()
        self._thread = list()

        for thread_id in range(0, self._workers):
            self._ticket.put(thread_id)
            self._thread.append(None)

    def wait_all(self):
        # Get all semaphore pool
        for _ in range(0, self._workers):
            self._pool.acquire()

    def run(self, task):
        # Get a worker resource from pool
        self._pool.acquire()

        # TODO Check thread table is useful.
        worker_id = self._ticket.get()

        if self._thread[worker_id]:
            self._thread[worker_id].join()

        self._thread[worker_id] = Thread(target=TaskScheduler._procedure,
                                         args=(self, worker_id, task))
        self._thread[worker_id].start()

    @staticmethod
    def _procedure(self, worker_id, task):
        # TODO implementing here
        thread = TaskThread()
        thread.run(task)

        self._manager.task_complete(thread)

        # TODO Check thread table is useful.
        self._ticket.put(worker_id)
        self._pool.release()
