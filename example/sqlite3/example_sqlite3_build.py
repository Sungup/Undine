import sqlite3
import json

#
# Queries
#

# 1. DB Create Queries
SQL_DB_CREATION = {
    'state_type': '''CREATE TABLE IF NOT EXISTS state_type
                     (state CHAR(1) PRIMARY KEY NOT NULL, name TEXT)''',
    'config': '''CREATE TABLE IF NOT EXISTS config
                 (cid INTEGER PRIMARY KEY, name TEXT, config TEXT)''',
    'input': '''CREATE TABLE IF NOT EXISTS input
                (iid INTEGER PRIMARY KEY, name TEXT, items TEXT)''',
    'worker': '''CREATE TABLE IF NOT EXISTS worker
                 (wid INTEGER PRIMARY KEY, name TEXT, command TEXT,
                  arguments TEXT, worker_dir TEXT)''',
    'task': '''CREATE TABLE IF NOT EXISTS task
               (tid INTEGER PRIMARY KEY, name TEXT,
                cid INTEGER NOT NULL REFERENCES config(cid),
                iid INTEGER NOT NULL REFERENCES input(iid),
                wid INTEGER NOT NULL REFERENCES worker(wid),
                state CHAR(1) NOT NULL DEFAULT('R')
                    REFERENCES state_type(state))''',
    'result': '''CREATE TABLE IF NOT EXISTS result
                 (tid INTEGER NOT NULL PRIMARY KEY REFERENCES task(tid),
                  content TEXT)''',
    'error': '''CREATE TABLE IF NOT EXISTS error
                (tid INTEGER NOT NULL PRIMARY KEY REFERENCES task(tid),
                 message TEXT)'''
}

# 2. state_type initialize query
SQL_STATE_ITEMS_INSERTION = 'INSERT INTO state_type VALUES (?, ?)'

STATE_ITEMS = [('R', 'ready'), ('I', 'issued'), ('D', 'done'),
               ('C', 'canceled'), ('F', 'failed')]

# 3. Each tables insertion query
SQL_CONFIG_INSERTION = "INSERT INTO config VALUES (?, ?, ?)"
SQL_INPUT_INSERTION = "INSERT INTO input VALUES (?, ?, ?)"
SQL_WORKER_INSERTION = "INSERT INTO worker VALUES (?, ?, ?, ?, ?)"
SQL_TASK_INSERTION = "INSERT INTO task VALUES (?, ?, ?, ?, ?, 'R')"

#
# Global environment
#

SQLITE3_DB_FILE = 'example.sqlite3'

TASK_CONFIG_FILE = '../json/task-config.json'
TASK_INPUT_FILE = '../json/task-inputs.json'
SYSTEM_CONFIG_FILE = '../../config/config-json.json'

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
w_wid_ = 0
w_name_ = 'worker1'
w_command_ = w_config_['worker_command']
w_arguments_ = w_config_['worker_arguments']
w_worker_dir_ = w_config_['worker_dir']

cursor_.execute(SQL_WORKER_INSERTION,
                (w_wid_, w_name_, w_command_, w_arguments_, w_worker_dir_))

#
# Insert configs
#
c_config_ = json.load(open(TASK_CONFIG_FILE, 'r'))
c_cid_ = 0

c_info_ = dict()

for name, content in c_config_.items():
    cursor_.execute(SQL_CONFIG_INSERTION, (c_cid_, name, json.dumps(content)))
    c_info_[c_cid_] = name

    c_cid_ += 1

#
# Insert inputs
#
i_config_ = json.load(open(TASK_INPUT_FILE, 'r'))
i_iid_ = 0

i_info_ = dict()

for name, items in i_config_.items():
    cursor_.execute(SQL_INPUT_INSERTION, (i_iid_, name, ','.join(items)))
    i_info_[i_iid_] = name

    i_iid_ += 1

#
# Insert tasks
#
t_tid_ = 0

for c_cid_, c_name_ in c_info_.items():
    for i_iid_, i_name_ in i_info_.items():
        cursor_.execute(SQL_TASK_INSERTION,
                        (t_tid_, '{}-{}'.format(c_name_, i_name_),
                         c_cid_, i_iid_, w_wid_))

        t_tid_ += 1

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
