import json
import pika
import uuid
import mysql.connector as mariadb

#
# Global environment
#

TASK_CONFIG_FILE = '../json/task-config.json'
TASK_INPUT_FILE = '../json/task-inputs.json'
SYSTEM_CONFIG_FILE = '../../config/config-json.json'

MARIA_DB_USER = 'undine'
MARIA_DB_PASSWD = 'password'
MARIA_DB_HOST = 'localhost'
MARIA_DB_NAME = 'undine'

RABBIT_MQ_USER = 'undine'
RABBIT_MQ_PASSWD = 'password'
RABBIT_MQ_HOST = 'localhost'
RABBIT_MQ_VHOST = 'undine'
RABBIT_MQ_QUEUE = 'task'

#
# Queries
#

# 1. DB Create Queries
SQL_DB_CREATION = {
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

# 2. state_type initialize query
SQL_STATE_ITEMS_INSERTION = 'INSERT INTO state_type VALUES (%s, %s)'

STATE_ITEMS = [('R', 'ready'), ('I', 'issued'), ('D', 'done'),
               ('C', 'canceled'), ('F', 'failed')]

# 3. Each tables insertion query
SQL_CONFIG_INSERTION = '''INSERT INTO config(cid, name, config)
                          VALUES (UNHEX(%(cid)s), %(name)s, %(config)s)'''
SQL_INPUT_INSERTION = '''INSERT INTO input(iid, name, items)
                         VALUES (UNHEX(%(iid)s), %(name)s, %(items)s)'''
SQL_WORKER_INSERTION = '''INSERT INTO worker(wid, name, command,
                                             arguments, worker_dir)
                          VALUES (UNHEX(%(wid)s), %(name)s, %(command)s,
                                  %(arguments)s, %(worker_dir)s)'''
SQL_TASK_INSERTION = '''INSERT INTO task(tid, name, cid, iid, wid)
                        VALUES (UNHEX(%(tid)s), %(name)s,
                                UNHEX(%(cid)s), UNHEX(%(iid)s),
                                UNHEX(%(wid)s))'''

#
# Connect into sqlite3 file
#
conn_ = mariadb.connect(host=MARIA_DB_HOST,
                        user=MARIA_DB_USER,
                        password=MARIA_DB_PASSWD,
                        database=MARIA_DB_NAME)

cursor_ = conn_.cursor()

#
# Connect into RabbitMQ
#
rb_credential_ = pika.PlainCredentials(RABBIT_MQ_USER, RABBIT_MQ_PASSWD)
rb_parameter_ = pika.ConnectionParameters(host=RABBIT_MQ_HOST,
                                          virtual_host=RABBIT_MQ_VHOST,
                                          credentials=rb_credential_)
rb_conn_ = pika.BlockingConnection(rb_parameter_)

rb_channel_ = rb_conn_.channel()
rb_property_ = pika.BasicProperties(delivery_mode = 2)

#
# Create database
#
for table_name, query in SQL_DB_CREATION.items():
    cursor_.execute('DROP TABLE IF EXISTS {}'.format(table_name))
    cursor_.execute(query)

cursor_.executemany(SQL_STATE_ITEMS_INSERTION, STATE_ITEMS)

rb_channel_.queue_delete(queue=RABBIT_MQ_QUEUE)
rb_channel_.queue_declare(queue=RABBIT_MQ_QUEUE, durable=True)

#
# Insert worker
#
w_config_ = json.load(open(SYSTEM_CONFIG_FILE, 'r'))['driver']

w_item_ = {
    'wid': str(uuid.uuid4()).replace('-', ''),
    'name': 'worker1',
    'command': w_config_['worker_command'],
    'arguments': w_config_['worker_arguments'],
    'worker_dir': w_config_['worker_dir']
}

cursor_.execute(SQL_WORKER_INSERTION, w_item_)

#
# Insert configs
#
c_config_ = json.load(open(TASK_CONFIG_FILE, 'r'))
c_info_ = dict()

for name, content in c_config_.items():
    cid = str(uuid.uuid4()).replace('-', '')
    c_item_ = {
        'cid': cid,
        'name': name,
        'config': json.dumps(content)
    }

    cursor_.execute(SQL_CONFIG_INSERTION, c_item_)
    c_info_[cid] = name

#
# Insert inputs
#
i_config_ = json.load(open(TASK_INPUT_FILE, 'r'))
i_info_ = dict()

for name, items in i_config_.items():
    iid = str(uuid.uuid4()).replace('-', '')
    i_item_ = {
        'iid': iid,
        'name': name,
        'items': ','.join(items)
    }

    cursor_.execute(SQL_INPUT_INSERTION, i_item_)
    i_info_[iid] = name

#
# Insert tasks
#
for c_cid_, c_name_ in c_info_.items():
    for i_iid_, i_name_ in i_info_.items():
        t_item_ = {
            'tid': str(uuid.uuid4()).replace('-', ''),
            'name': '{}-{}'.format(c_name_, i_name_),
            'cid': c_cid_,
            'iid': i_iid_,
            'wid': w_item_['wid']
        }

        cursor_.execute(SQL_TASK_INSERTION, t_item_)

        rb_channel_.basic_publish(exchange= '',
                                  routing_key=RABBIT_MQ_QUEUE,
                                  body=json.dumps(t_item_),
                                  properties=rb_property_)

#
# Check
#
cursor_.execute("SELECT COUNT(tid) FROM task WHERE state = 'R'")
result = cursor_.fetchone()

print(result[0])

#
# Commit and Disconnect
#
conn_.commit()
conn_.close()

rb_conn_.close()
