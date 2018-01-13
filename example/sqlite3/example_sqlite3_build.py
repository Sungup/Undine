import json
import sqlite3
import uuid

#
# Global environment
#

TASK_CONFIG_FILE = '../json/task-config.json'
TASK_INPUT_FILE = '../json/task-inputs.json'
SYSTEM_CONFIG_FILE = '../../config/config-json.json'

SQLITE3_DB_FILE = 'example.sqlite3'

#
# Queries
#

# 1. DB Create Queries
SQL_DB_CREATION = {
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

# 2. state_type initialize query
SQL_STATE_ITEMS_INSERTION = 'INSERT INTO state_type VALUES (?, ?)'

STATE_ITEMS = [('R', 'ready'), ('I', 'issued'), ('D', 'done'),
               ('C', 'canceled'), ('F', 'failed')]

# 3. Each tables insertion query
SQL_CONFIG_INSERTION = '''INSERT INTO config
                          VALUES (:cid, :name, :config)'''
SQL_INPUT_INSERTION = "INSERT INTO input VALUES (:iid, :name, :items)"
SQL_WORKER_INSERTION = '''INSERT INTO worker
                          VALUES (:wid, :name, :command,
                                  :arguments, :worker_dir)'''
SQL_TASK_INSERTION = '''INSERT INTO task
                        VALUES (:tid, :name,
                                :cid, :iid, :wid, 'R')'''

#
# Connect into sqlite3 file
#
conn_ = sqlite3.connect(SQLITE3_DB_FILE)

cursor_ = conn_.cursor()

#
# Create database
#
for table_name, query in SQL_DB_CREATION.items():
    cursor_.execute('DROP TABLE IF EXISTS {}'.format(table_name))
    cursor_.execute(query)

cursor_.executemany(SQL_STATE_ITEMS_INSERTION, STATE_ITEMS)

#
# Insert worker
#
w_config_ = json.load(open(SYSTEM_CONFIG_FILE, 'r'))['driver']

w_item_ = {
    'wid': str(uuid.uuid4()).replace('-', '').upper(),
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
    cid = str(uuid.uuid4()).replace('-', '').upper()
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
    iid = str(uuid.uuid4()).replace('-', '').upper()
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
            'tid': str(uuid.uuid4()).replace('-', '').upper(),
            'name': '{}-{}'.format(c_name_, i_name_),
            'cid': c_cid_,
            'iid': i_iid_,
            'wid': w_item_['wid']
        }

        cursor_.execute(SQL_TASK_INSERTION, t_item_)

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
