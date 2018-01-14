from argparse import ArgumentParser
from collections import namedtuple
from undine.client.mariadb_client import MariaDbClient
from undine.database.mariadb import MariaDbConnector
from undine.database.rabbitmq import RabbitMQConnector

import itertools
import json


def db_build(rabbitmq_config, mariadb_config):
    build_query = {
        'state_type': '''CREATE TABLE IF NOT EXISTS state_type
                         (state CHAR(1) PRIMARY KEY NOT NULL, name TEXT)''',
        'config': '''CREATE TABLE IF NOT EXISTS config
                     (cid BINARY(16) PRIMARY KEY, name TEXT, config TEXT,
                      issued DATETIME DEFAULT CURRENT_TIMESTAMP)''',
        'input': '''CREATE TABLE IF NOT EXISTS input
                    (iid BINARY(16) PRIMARY KEY, name TEXT, items TEXT,
                     issued DATETIME DEFAULT CURRENT_TIMESTAMP)''',
        'worker': '''CREATE TABLE IF NOT EXISTS worker
                     (wid BINARY(16) PRIMARY KEY, name TEXT, command TEXT,
                      arguments TEXT, worker_dir TEXT,
                      issued DATETIME DEFAULT CURRENT_TIMESTAMP)''',
        'task': '''CREATE TABLE IF NOT EXISTS task
                   (tid BINARY(16) PRIMARY KEY, name TEXT,
                    cid BINARY(16) NOT NULL REFERENCES config(cid),
                    iid BINARY(16) NOT NULL REFERENCES input(iid),
                    wid BINARY(16) NOT NULL REFERENCES worker(wid),
                    issued DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated DATETIME ON UPDATE CURRENT_TIMESTAMP,
                    state CHAR(1) NOT NULL DEFAULT('R')
                        REFERENCES state_type(state))''',
        'result': '''CREATE TABLE IF NOT EXISTS result
                     (tid BINARY(16) NOT NULL PRIMARY KEY REFERENCES task(tid),
                      reported DATETIME DEFAULT CURRENT_TIMESTAMP,
                      content TEXT)''',
        'error': '''CREATE TABLE IF NOT EXISTS error
                    (tid BINARY(16) NOT NULL PRIMARY KEY REFERENCES task(tid),
                     informed DATETIME DEFAULT CURRENT_TIMESTAMP,
                     message TEXT)'''
    }

    state_insertion = 'INSERT INTO state_type VALUES (%s, %s)'

    state_items = [('R', 'ready'), ('I', 'issued'), ('D', 'done'),
                   ('C', 'canceled'), ('F', 'failed')]

    # 0. Connect to server
    #
    # But, rabbitmq just call constructor to rebuild the queue

    mariadb = MariaDbConnector(mariadb_config)
    _ = RabbitMQConnector(rabbitmq_config, consumer=False, rebuild=True)

    queries = list()
    for name, query in build_query.items():
        queries.append(mariadb.sql_item('DROP TABLE IF EXISTS {}'.format(name)))
        queries.append(mariadb.sql_item(query))

    queries.extend([mariadb.sql_item(state_insertion, item)
                    for item in state_items])

    mariadb.execute_multiple_dml(queries)


def data_filling(rabbitmq_config, mariadb_config):
    config_info = '../json/task-config.json'
    input_info = '../json/task-inputs.json'
    worker_info = '../../config/config-json.json'

    # 0. Connect to server
    client = MariaDbClient(rabbitmq_config, mariadb_config)

    # 1. Insert worker
    worker = json.load(open(worker_info, 'r'))['driver']

    wid = client.publish_worker(name='worker1',
                                command=worker['worker_command'],
                                arguments=worker['worker_arguments'],
                                worker_dir=worker['worker_dir'])

    # 2. Insert config
    config_items = dict()

    for name, content in json.load(open(config_info, 'r')).items():
        config_items[client.publish_config(name, content)] = name

    # 3. Insert inputs
    input_items = dict()

    for name, items in json.load(open(input_info, 'r')).items():
        input_items[client.publish_input(name, items)] = name

    # 4. Insert tasks
    KeyMap = namedtuple('KeyMap', ['config', 'input'])

    for item in [KeyMap(*value)
                 for value in itertools.product(config_items, input_items)]:
        client.publish_task(name='{0}-{1}'.format(config_items[item.config],
                                                  input_items[item.input]),
                            cid=item.config, iid=item.input, wid=wid)


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

    parser.add_argument('-b', '--build', action='store_false',
                        dest='build', default=True, help="Don't Build tables")

    parser.add_argument('-f', '--fill', action='store_true',
                        dest='fill', default=False, help='Filling tables')

    options = parser.parse_args()

    # 2. Run each items
    if options.build:
        db_build(_rabbitmq_config, _mariadb_config)

    if options.fill:
        data_filling(_rabbitmq_config, _mariadb_config)
