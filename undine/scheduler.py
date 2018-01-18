from multiprocessing import Queue
from threading import Semaphore, Thread, Lock
from undine.database.rabbitmq import RabbitMQRpcServer
from undine.utils.system import System

import subprocess
import undine.utils.logging as logging


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


class SchedulerState:
    def __init__(self):
        self._lock = Lock()
        self._on_the_fly = set()

    def query(self, cmd):
        with self._lock:
            print(cmd)

    def add(self, tid):
        with self._lock:
            self._on_the_fly.add(tid)

    def remove(self, tid):
        with self._lock:
            self._on_the_fly.remove(tid)


class TaskScheduler:
    _SCHEDULER_LOGGER_NAME = 'undine-scheduler'
    _SCHEDULER_LOGGER_PATH = '/tmp/{}.log'.format(_SCHEDULER_LOGGER_NAME)
    _SCHEDULER_LOGGER_LEVEL = 'ERROR'

    def _log_string(self, name, task):
        if logging.is_debug(self._logger):
            return "tid({1}) {0}\n\tcommand -> {2}".format(name,
                                                           task.tid, task.cmd)
        else:
            return "tid({1}) {0}".format(name, task.tid)

    def __init__(self, manager, config, rpc_queue):
        system_cpu = System.cpu_cores() - 1
        config_cpu = int(config.setdefault('max_cpu', '0'))

        self._workers = max(system_cpu, config_cpu)
        self._manager = manager
        self._pool = Semaphore(self._workers)

        # Initialize SchedulerState
        self._state = SchedulerState()

        # Message queue initialize
        if rpc_queue:
            rpc_name = 'rpc-{}'.format(System.host_info().ipv4)
            self._rpc = RabbitMQRpcServer(rpc_queue, rpc_name,
                                          lambda body: self._state.query(body))

        # Create logger instance
        log_path = config.setdefault('log_file', self._SCHEDULER_LOGGER_PATH)
        log_level = config.setdefault('log_level', self._SCHEDULER_LOGGER_LEVEL)

        self._logger = logging.get_logger(self._SCHEDULER_LOGGER_NAME,
                                          log_path, log_level)

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

        self._state.add(task.tid)

        thread.start()

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
