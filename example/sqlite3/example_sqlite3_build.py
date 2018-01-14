from collections import namedtuple
from undine.client.sqlite_client import SQLiteClient
from undine.database.sqlite import SQLiteConnector

import itertools
import json


def db_build(sqlite_config):
    build_query = {
        'state_type': '''CREATE TABLE IF NOT EXISTS state_type
                         (state CHAR(1) PRIMARY KEY NOT NULL, name TEXT)''',
        'config': '''CREATE TABLE IF NOT EXISTS config
                     (cid TEXT PRIMARY KEY, name TEXT, config TEXT)''',
        'input': '''CREATE TABLE IF NOT EXISTS input
                    (iid TEXT PRIMARY KEY, name TEXT, items TEXT)''',
        'worker': '''CREATE TABLE IF NOT EXISTS worker
                     (wid TEXT PRIMARY KEY, name TEXT, command TEXT,
                      arguments TEXT, worker_dir TEXT)''',
        'task': '''CREATE TABLE IF NOT EXISTS task
                   (tid TEXT PRIMARY KEY, name TEXT,
                    cid TEXT NOT NULL REFERENCES config(cid),
                    iid TEXT NOT NULL REFERENCES input(iid),
                    wid TEXT NOT NULL REFERENCES worker(wid),
                    state CHAR(1) NOT NULL DEFAULT('R')
                        REFERENCES state_type(state))''',
        'result': '''CREATE TABLE IF NOT EXISTS result
                     (tid TEXT NOT NULL PRIMARY KEY REFERENCES task(tid),
                      content TEXT)''',
        'error': '''CREATE TABLE IF NOT EXISTS error
                    (tid TEXT NOT NULL PRIMARY KEY REFERENCES task(tid),
                     message TEXT)'''
    }

    state_insertion = 'INSERT INTO state_type VALUES (?, ?)'

    state_items = [('R', 'ready'), ('I', 'issued'), ('D', 'done'),
                   ('C', 'canceled'), ('F', 'failed')]

    # 0. Connect to file
    sqlite = SQLiteConnector(sqlite_config)

    queries = list()
    for name, query in build_query.items():
        queries.append(sqlite.sql_item('DROP TABLE IF EXISTS {}'.format(name)))
        queries.append(sqlite.sql_item(query))

    queries.extend([sqlite.sql_item(state_insertion, item)
                    for item in state_items])

    sqlite.execute_multiple_dml(queries)


def data_filling(sqlite_config):
    config_info = '../json/task-config.json'
    input_info = '../json/task-inputs.json'
    worker_info = '../../config/config-json.json'

    # 0. Connect to server
    client = SQLiteClient(sqlite_config)

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
    _sqlite_config = {
        'db_file': 'example.sqlite3'
    }

    db_build(_sqlite_config)
    data_filling(_sqlite_config)
