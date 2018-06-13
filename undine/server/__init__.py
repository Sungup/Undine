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


class ConfigParser:
    Option = namedtuple('Options',
                        ['short', 'long', 'dest', 'action', 'meta', 'help',
                         'required'])
    _OPTIONS = [
        Option('-c', '--config', 'config_file', 'store', 'PATH',
               'Config file path', True)
    ]

    @staticmethod
    def parse(options = _OPTIONS):
        parser = ArgumentParser(description='Undine task manager.')

        for item in options:
            parser.add_argument(item.short, item.long, metavar=item.meta,
                                dest=item.dest, action=item.action,
                                help=item.help, required=item.required)

        return parser.parse_args()

    @staticmethod
    def load_config():
        config = ConfigParser.parse()

        if not os.path.isfile(config.config_file):
            raise UndineException('No such config file at {}.'.format(
                config.config_file))

        return json.load(open(config.config_file, 'r'))


class TaskManager:
    _DEFAULT_OPTS = {
        'config_dir': '/tmp/config',
        'result_dir': '/tmp/result',
        'result_ext': '.log',
        'input_dir': None
    }

    _DRIVER_ERR_MESSAGE = 'Driver configuration should be set in config file'

    def __init__(self, config):
        self._config = self._default_opts(config['manager'])

        if 'driver' not in config:
            raise UndineException(self._DRIVER_ERR_MESSAGE)

        rpc_config = config.setdefault('rpc', None)
        task_queue_config = config.setdefault('task_queue', None)
        scheduler_config = config.setdefault('scheduler', dict())
        config_dir = self._config['config_dir']

        self._driver = TaskDriverFactory.create(config=config['driver'],
                                                config_dir=config_dir,
                                                task_queue =task_queue_config)

        self._scheduler = TaskScheduler(manager=self, config=scheduler_config)

        self._info = {}
        self._start_at = datetime.now()

        if rpc_config:
            self._rpc = RpcDaemon(rpc_config)

            # Create information
            host_info = System.host_info()
            self._info = {
                'hostname': host_info.name,
                'address': host_info.ipv4,
                'total_cpu': System.cpu_cores(),
                'max_cpu': self._scheduler.max_cpu,
                'start': str(self._start_at)
            }

            self._rpc.register('server', self.stats_procedure)
            self._rpc.register('scheduler', self._scheduler.stats_procedure)

            self._rpc.start()

    def stats_procedure(self, *_args, **_kwargs):
        # Currently args and kwargs not in use.
        info = self._info.copy()
        info['uptime'] = str(datetime.now() - self._start_at)

        return info

    def _default_opts(self, config):
        return dict([(k, config.setdefault(k, v))
                     for k, v in self._DEFAULT_OPTS.items()])

    def _run(self):
        task_driver = self._driver
        task_scheduler = self._scheduler

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
                                   **self._config))

            # Before terminate, wait all threads
            task_scheduler.wait_all()

        except (KeyboardInterrupt, SystemExit):
            # Stop RPC server
            if self._rpc:
                self._rpc.stop()

    def task_complete(self, task_obj):
        if not isinstance(task_obj, Task):
            raise UndineException('Unexpected task object')

        if task_obj.is_success:
            self._driver.done(task_obj.tid, task_obj.result, task_obj.report)
        else:
            self._driver.fail(task_obj.tid, task_obj.message)

    @staticmethod
    def run():
        TaskManager(ConfigParser.load_config())._run()
