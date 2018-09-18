from argparse import ArgumentParser
from collections import namedtuple
from datetime import datetime
from undine.server.scheduler import TaskScheduler
from undine.server.task import Task
from undine.server.driver import TaskDriverFactory
from undine.server.rpc import RpcDaemon
from undine.utils.exception import UndineException
from undine.utils.system import System

import os
import json


def __load_config():
    _default = '/etc/aria/undine.json'

    parser = ArgumentParser(description='Undine task manager.')

    parser.add_argument('-c', '--config', dest='config_file', metavar='PATH',
                        help='Config file path [default: '.format(_default),
                        action='store', default=_default, required=True)

    opts = parser.parse_args()

    if not os.path.isfile(config.config_file):
        raise UndineException('No such file at {}.'format(config.config_file))

    return json.load(open(config.config_file, 'r'))


class TaskManager:
    __DEFAULTOPTS = {
        'config_dir': '/tmp/config',
        'result_dir': '/tmp/result',
        'result_ext': '.log',
        'input_dir': None
    }

    _DRIVER_ERR_MESSAGE = 'Driver configuration should be set in config file'

    def __init__(self, config):
        self.__config = self.__default_opts(config['manager'])

        if 'driver' not in config:
            raise UndineException(self.__DRIVER_ERR_MESSAGE)

        rpc_config = config.setdefault('rpc', None)
        task_queue_config = config.setdefault('task_queue', None)
        scheduler_config = config.setdefault('scheduler', dict())
        config_dir = self.__config['config_dir']

        self.__driver = TaskDriverFactory.create(config=config['driver'],
                                                 config_dir=config_dir,
                                                 task_queue =task_queue_config)

        self.__scheduler = TaskScheduler(manager=self, config=scheduler_config)

        self.__info = {}
        self.__start_at = datetime.now()

        if rpc_config:
            self.__rpc = RpcDaemon(rpc_config)

            # Create information
            host_info = System.host_info()
            self.__info = {
                'hostname': host_info.name,
                'address': host_info.ipv4,
                'total_cpu': System.cpu_cores(),
                'max_cpu': self.__scheduler.max_cpu,
                'start': str(self.__start_at)
            }

            self.__rpc.register('server', self.stats_procedure)
            self.__rpc.register('scheduler', self.__scheduler.stats_procedure)

            self.__rpc.start()

    def stats_procedure(self, *_args, **_kwargs):
        # Currently args and kwargs not in use.
        info = self.__info.copy()
        info['uptime'] = str(datetime.now() - self.__start_at)

        return info

    def __default_opts(self, config):
        return dict([(k, config.setdefault(k, v))
                     for k, v in self.__DEFAULTOPTS.items()])

    def __run(self):
        task_driver = self.__driver
        task_scheduler = self.__scheduler

        try:
            while task_scheduler.is_ready() and task_driver.is_ready():
                task_info = task_driver.fetch()

                # Received task is already issued or canceled.
                if not task_info:
                    continue

                # Notify to driver this task will control this manager
                task_driver.preempt(task_info.tid)

                config = task_driver.config(task_info.cid)
                worker = task_driver.worker(task_info.wid)
                inputs = task_driver.inputs(task_info.iid)

                task_scheduler.run(Task(task_info, worker, config, inputs,
                                   **self.__config))

            # Before terminate, wait all threads
            task_scheduler.wait_all()

        except (KeyboardInterrupt, SystemExit):
            # Stop RPC server
            if self.__rpc:
                self.__rpc.stop()

    def task_complete(self, task_obj):
        if not isinstance(task_obj, Task):
            raise UndineException('Unexpected task object')

        if task_obj.is_success:
            self.__driver.done(task_obj.tid, task_obj.result, task_obj.report)
        else:
            self.__driver.fail(task_obj.tid, task_obj.message)

    @staticmethod
    def run():
        TaskManager(__load_config()).__run()
