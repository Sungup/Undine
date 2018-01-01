from undine.utils.system import System
from threading import Semaphore, Thread
from multiprocessing import Queue

import subprocess


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
    def success(self):
        return self._state == 0


class TaskScheduler:
    def __init__(self, manager, config):
        system_cpu = System.cpu_cores() - 1
        config_cpu = int(config.setdefault('max_cpu', '0'))

        self._workers = max(system_cpu, config_cpu)
        self._manager = manager
        self._pool = Semaphore(self._workers)

        # ==================================================
        # TODO Check thread table is useful.
        self._ticket = Queue()
        self._thread = list()

        for thread_id in range(0, self._workers):
            self._ticket.put(thread_id)
            self._thread.append(None)
        # ==================================================

    def wait_all(self):
        # Get all semaphore pool
        for _ in range(0, self._workers):
            self._pool.acquire()

    def run(self, task):
        # Get a worker resource from pool
        self._pool.acquire()

        # ==================================================
        # TODO Check thread table is useful.
        worker_id = self._ticket.get()

        if self._thread[worker_id]:
            self._thread[worker_id].join()

        thread = Thread(target=TaskScheduler._procedure,
                        args=(self, task, worker_id))

        self._thread[worker_id] = thread
        # ==================================================
        # else condition method
        # thread = Thread(target=TaskScheduler._procedure, args=(self, task))
        # ==================================================

        thread.start()

    # ==================================================
    # TODO Check thread table is useful.
    # @staticmethod
    # def _procedure(self, task):
    # ==================================================
    @staticmethod
    def _procedure(self, task, worker_id):
        thread = TaskThread()
        thread.run(task)

        if thread.success:
            task.success(thread.result_message)
        else:
            task.fail(thread.error_message)

        self._manager.task_complete(thread)

        # ==================================================
        # TODO Check thread table is useful.
        self._ticket.put(worker_id)
        self._pool.release()
        # ==================================================
