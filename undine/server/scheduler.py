from datetime import datetime
from multiprocessing import Queue
from threading import Semaphore, Thread, Lock
from undine.utils.system import System
from undine.utils import logging
from time import sleep

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


class _SchedulerStats:
    def __init__(self, worker_count):
        self._worker_count = worker_count
        self._lock = Lock()
        self._on_the_fly = dict()

    def query(self):
        with self._lock:
            run = len(self._on_the_fly)

            return {'run_workers': run,
                    'utilization': run / self._worker_count * 100.0,
                    'list': [{'id': task, 'time': str(datetime.now() - time)}
                             for task, time in self._on_the_fly.items()]}

    def add(self, tid):
        with self._lock:
            self._on_the_fly[tid] = datetime.now()

    def remove(self, tid):
        with self._lock:
            self._on_the_fly.pop(tid, None)


class TaskScheduler:
    _SCHEDULER_LOGGER_NAME = 'undine-scheduler'
    _SCHEDULER_LOGGER_PATH = '/tmp/{}.log'.format(_SCHEDULER_LOGGER_NAME)
    _SCHEDULER_LOGGER_LEVEL = 'ERROR'
    _SCHEDULER_TASK_INTERVAL = '1'
    _MAX_CPU = 999999

    def _log_string(self, name, task):
        if logging.is_debug(self._logger):
            return "tid({1}) {0}\n\tcommand -> {2}".format(name,
                                                           task.tid, task.cmd)
        else:
            return "tid({1}) {0}".format(name, task.tid)

    def __init__(self, manager, config):
        system_cpu = System.cpu_cores() - 1
        config_cpu = int(config.setdefault('max_cpu', self._MAX_CPU))
        interval = int(config.setdefault('task_interval',
                                         self._SCHEDULER_TASK_INTERVAL))

        # If the configured # of CPUs is 0, change the config_cpu to MAX_CPU.
        # value
        if config_cpu == 0:
            config_cpu = self._MAX_CPU

        self._workers = min(system_cpu, config_cpu)
        self._manager = manager
        self._pool = Semaphore(self._workers)
        self._task_interval = interval

        # Initialize SchedulerState
        self._state = _SchedulerStats(self._workers)

        # Create logger instance
        log_path = config.setdefault('log_file', self._SCHEDULER_LOGGER_PATH)
        log_level = config.setdefault('log_level', self._SCHEDULER_LOGGER_LEVEL)

        self._logger = logging.get_logger(self._SCHEDULER_LOGGER_NAME,
                                          log_path, log_level)

        # ==================================================
        # TODO Check thread table is useful.
        self._ticket = Queue()
        self._thread = dict()

        for thread_id in range(0, self._workers):
            self._ticket.put(thread_id)
        # ==================================================

    @property
    def max_cpu(self):
        return self._workers

    def is_ready(self):
        self._pool.acquire()
        self._pool.release()

        return True

    def wait_all(self):
        # Get all semaphore pool
        for _ in range(0, self._workers):
            self._pool.acquire()

    def run(self, task):
        # Get a worker resource from pool
        self._pool.acquire()

        # ==================================================
        # TODO Check thread table is useful.
        thread_id = self._ticket.get()

        if thread_id in self._thread:
            self._thread[thread_id].join()
            del self._thread[thread_id]

        thread = Thread(target=TaskScheduler._procedure,
                        args=(self, task, thread_id))

        self._thread[thread_id] = thread
        # ==================================================
        # else condition method
        # thread = Thread(target=TaskScheduler._procedure, args=(self, task))
        # ==================================================

        self._state.add(task.tid)

        thread.start()

        # Sleep during task_interval to avoid the task interference.
        sleep(self._task_interval)

    def stats_procedure(self, *_args, **_kwargs):
        # Currently args and kwargs not in use.
        return self._state.query()

    # ==================================================
    # TODO Check thread table is useful.
    # @staticmethod
    # def _procedure(self, task):
    # ==================================================
    @staticmethod
    def _procedure(self, task, worker_id):
        thread = TaskThread()

        self._logger.info(self._log_string("Task start", task))

        thread.run(task.cmd)

        if thread.success:
            task.success(thread.result_message)
            self._logger.info(self._log_string("Task complete", task))
        else:
            task.fail(thread.error_message)
            self._logger.info(self._log_string("Task fail", task))

        self._state.remove(task.tid)

        self._manager.task_complete(task)

        # ==================================================
        # TODO Check thread table is useful.
        self._ticket.put(worker_id)
        self._pool.release()
        # ==================================================
