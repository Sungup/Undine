from collections import namedtuple
from argparse import ArgumentParser
from undine.driver.driver_factory import TaskDriverFactory
from undine.utils.exception import UndineException
from undine.process import TaskScheduler
from undine.task import Task

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

        rpc_queue = config.setdefault('rpc_queue', None)
        task_queue = config.setdefault('task_queue', None)
        scheduler = config.setdefault('scheduler', dict())
        config_dir = self._config['config_dir']

        self._driver = TaskDriverFactory.create(config=config['driver'],
                                                config_dir=config_dir,
                                                task_queue =task_queue)

        self._scheduler = TaskScheduler(manager=self,
                                        config=scheduler,
                                        rpc_queue=rpc_queue)

    def _default_opts(self, config):
        return dict([(k, config.setdefault(k, v))
                     for k, v in self._DEFAULT_OPTS.items()])

    def _run(self):
        driver = self._driver
        scheduler = self._scheduler

        while driver.wait_others():
            task = driver.fetch()

            # Notify to driver this task will control this manager
            driver.preempt(task.tid)

            config = driver.config(task.cid)
            worker = driver.worker(task.wid)
            inputs = driver.inputs(task.iid)

            scheduler.run(Task(task.tid, worker, config, inputs,
                               **self._config))

        # Before terminate, wait all threads
        scheduler.wait_all()

    def task_complete(self, task):
        if not isinstance(task, Task):
            raise UndineException('Unexpected task object')

        if task.is_success:
            self._driver.done(task.tid, task.result)
        else:
            self._driver.fail(task.tid, task.message)

    @staticmethod
    def run():
        TaskManager(ConfigParser.load_config())._run()


if __name__ == '__main__':
    TaskManager.run()
