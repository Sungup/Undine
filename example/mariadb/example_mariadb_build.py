from argparse import ArgumentParser
from collections import namedtuple
from undine.api.database.mariadb import MariaDbClient
from undine.database.mariadb import MariaDbConnector
from undine.database.rabbitmq import RabbitMQConnector

import itertools
import json
import names
import random


def data_filling(rabbitmq_config, mariadb_config, input_type):
    config_info = '../json/task-config.json'
    input_info = '../json/task-inputs.json'
    worker_info = '../../tmp/config/config-json.json'

    # 0. Connect to server
    client = MariaDbClient(rabbitmq_config, mariadb_config)

    # 1. Insert Mission
    mid = client.publish_mission(names.get_full_name(),
                                 'noreply@example.com',
                                 'Sample Mission\n\n Detail descriptions')

    # 2. Insert worker
    worker = json.load(open(worker_info, 'r'))['driver']

    args = '--type {type} {origin}'.format(type=input_type,
                                           origin=worker['worker_arguments'])

    wid = client.publish_worker(name='worker1',
                                command=worker['worker_command'],
                                arguments=args,
                                worker_dir=worker['worker_dir'],
                                file_input=bool(input_type == 'file'))

    # 3. Insert config
    config_items = dict()

    for name, content in json.load(open(config_info, 'r')).items():
        config_items[client.publish_config(name, content)] = name

    # 4. Insert inputs
    input_items = dict()

    for name, items in json.load(open(input_info, 'r')).items():
        input_items[client.publish_input(name, items)] = name

    # 5. Insert tasks
    KeyMap = namedtuple('KeyMap', ['config', 'input'])

    for item in [KeyMap(*value)
                 for value in itertools.product(config_items, input_items)]:
        client.publish_task(name='{0}-{1}'.format(config_items[item.config],
                                                  input_items[item.input]),
                            cid=item.config, iid=item.input, wid=wid, mid=mid,
                            report=random.randrange(0, 2))


if __name__ == '__main__':
    _mariadb_config = {
        'host': 'localhost',
        'database': 'undine',
        'user': 'undine',
        'password': 'password'
    }

    _rabbitmq_config = {
        'host': 'localhost',
        'vhost': 'undine',
        'queue': 'task',
        'user': 'undine',
        'password': 'password'
    }

    # 1. Parse program arguments
    parser = ArgumentParser(description='MariaDB example builder')

    parser.add_argument('--type', dest='input_type',
                        action='store', choices=['file', 'id'],
                        default='file', help='Input argument type')

    options = parser.parse_args()

    # 2. Run each items
    data_filling(_rabbitmq_config, _mariadb_config, options.input_type)
