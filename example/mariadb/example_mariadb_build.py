from argparse import ArgumentParser
from collections import namedtuple
from undine.api.database.mariadb import MariaDbClient
from undine.database.mariadb import MariaDbConnector
from undine.database.rabbitmq import RabbitMQConnector

import itertools
import json
import names
import random


def db_build(rabbitmq_config, mariadb_config):
    create_table = {
        'state_type': '''
            CREATE TABLE IF NOT EXISTS state_type
            (state CHAR(1) PRIMARY KEY NOT NULL, name TEXT)
        ''',
        'mission': '''
            CREATE TABLE IF NOT EXISTS mission 
            (mid BINARY(16) PRIMARY KEY, name TEXT, email TEXT,
             description TEXT,
             issued DATETIME DEFAULT CURRENT_TIMESTAMP)
        ''',
        'config': '''
            CREATE TABLE IF NOT EXISTS config
            (cid BINARY(16) PRIMARY KEY, name TEXT, config TEXT,
             issued DATETIME DEFAULT CURRENT_TIMESTAMP)
        ''',
        'input': '''
            CREATE TABLE IF NOT EXISTS input
            (iid BINARY(16) PRIMARY KEY, name TEXT, items TEXT,
             issued DATETIME DEFAULT CURRENT_TIMESTAMP)
        ''',
        'worker': '''
            CREATE TABLE IF NOT EXISTS worker
            (wid BINARY(16) PRIMARY KEY, name TEXT, command TEXT,
             arguments TEXT, worker_dir TEXT,
             issued DATETIME DEFAULT CURRENT_TIMESTAMP)
        ''',
        'task': '''
            CREATE TABLE IF NOT EXISTS task
            (tid BINARY(16) PRIMARY KEY, name TEXT,
             cid BINARY(16) NOT NULL REFERENCES config(cid),
             iid BINARY(16) NOT NULL REFERENCES input(iid),
             wid BINARY(16) NOT NULL REFERENCES worker(wid),
             mid BINARY(16) DEFAULT(NULL) REFERENCES mission(mid),
             issued DATETIME DEFAULT CURRENT_TIMESTAMP,
             updated DATETIME ON UPDATE CURRENT_TIMESTAMP,
             host VARCHAR(255),
             ip INT UNSIGNED,
             reportable BOOLEAN NOT NULL DEFAULT(TRUE),
             state CHAR(1) NOT NULL DEFAULT('R') REFERENCES state_type(state),
             KEY state_key (mid, state))
        ''',
        'result': '''
            CREATE TABLE IF NOT EXISTS result
            (tid BINARY(16) NOT NULL REFERENCES task(tid),
             reported DATETIME DEFAULT CURRENT_TIMESTAMP,
             content TEXT)
        ''',
        'error': '''
            CREATE TABLE IF NOT EXISTS error
            (tid BINARY(16) NOT NULL REFERENCES task(tid),
             informed DATETIME DEFAULT CURRENT_TIMESTAMP,
             message TEXT)
         '''
    }

    dashboard_view = '''
        CREATE OR REPLACE VIEW mission_dashboard AS
        SELECT HEX(m.mid) AS mid, m.name, m.email, m.description,
               COUNT(CASE WHEN t.state = 'R' THEN 1 END) AS ready,
               COUNT(CASE WHEN t.state = 'I' THEN 1 END) AS issued,
               COUNT(CASE WHEN t.state = 'D' THEN 1 END) AS done,
               COUNT(CASE WHEN t.state = 'C' THEN 1 END) AS canceled,
               COUNT(CASE WHEN t.state = 'F' THEN 1 END) AS failed,
               NOT COUNT(CASE WHEN t.state != 'D' THEN 1 END) AS complete,
               m.issued AS issued_at
          FROM mission AS m, task AS t
         WHERE m.mid = t.mid
         GROUP BY m.mid
         ORDER BY m.issued DESC
    '''

    task_list_view = '''
        CREATE OR REPLACE VIEW task_list AS
        SELECT t.mid, HEX(tid) AS tid, t.name, t.host, INET_NTOA(t.ip) as ip,
               t.issued, t.updated, s.name AS state, t.reportable,
               t.cid, t.iid, t.wid
          FROM task AS t, state_type s
         WHERE t.state = s.state
         ORDER BY issued ASC
    '''

    state_insertion = 'INSERT INTO state_type VALUES (%s, %s)'

    state_items = [('R', 'ready'), ('I', 'issued'), ('D', 'done'),
                   ('C', 'canceled'), ('F', 'failed')]

    # 0. Connect to server
    #
    # But, rabbitmq just call constructor to rebuild the queue

    mariadb = MariaDbConnector(mariadb_config)
    _ = RabbitMQConnector(rabbitmq_config, consumer=False, rebuild=True)

    queries = list()
    for name, query in create_table.items():
        queries.append(mariadb.sql_item('DROP TABLE IF EXISTS {}'.format(name)))
        queries.append(mariadb.sql_item(query))

    queries.extend([mariadb.sql_item(state_insertion, item)
                    for item in state_items])

    queries.append(mariadb.sql_item(dashboard_view))
    queries.append(mariadb.sql_item(task_list_view))

    mariadb.execute_multiple_dml(queries)


def data_filling(rabbitmq_config, mariadb_config):
    config_info = '../json/task-config.json'
    input_info = '../json/task-inputs.json'
    worker_info = '../../config/config-json.json'

    # 0. Connect to server
    client = MariaDbClient(rabbitmq_config, mariadb_config)

    # 1. Insert Mission
    mid = client.publish_mission(names.get_full_name(),
                                 'noreply@example.com',
                                 'Sample Mission\n\n Detail descriptions')

    # 2. Insert worker
    worker = json.load(open(worker_info, 'r'))['driver']

    wid = client.publish_worker(name='worker1',
                                command=worker['worker_command'],
                                arguments=worker['worker_arguments'],
                                worker_dir=worker['worker_dir'])

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
