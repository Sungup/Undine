from undine.api.database.base_client import BaseNetworkClient
from undine.database.mariadb import MariaDbConnector


class MariaDbClient(BaseNetworkClient):
    _QUERY = {
        'mission': '''
            INSERT INTO mission(mid, name, email, description, issued)
            VALUES(%(mid)s, %(name)s, %(email)s, %(description)s, NOW())
        ''',
        'task': '''
            INSERT INTO task(tid, name, cid, iid, wid, mid,
                             reportable, issued, updated, host, ip, state)
            VALUES(%(tid)s, %(name)s, %(cid)s, %(iid)s, %(wid)s, %(mid)s,
                   %(reportable)s, NOW(), NULL, NULL, NULL, 'R')
        ''',
        'worker': '''
            INSERT INTO worker(wid, name, command, arguments,
                               worker_dir, file_input, issued)
            VALUES(%(wid)s, %(name)s, %(command)s,
                   %(arguments)s, %(worker_dir)s, %(file_input)s, NOW())
        ''',
        'input': '''
            INSERT INTO input(iid, name, items, issued)
            VALUES(%(iid)s, %(name)s, %(items)s, NOW())
        ''',
        'config': '''
            INSERT INTO config(cid, name, config, issued)
            VALUES (%(cid)s, %(name)s, %(config)s, NOW())
        '''
    }

    def __init__(self, task_queue, config):
        BaseNetworkClient.__init__(self, task_queue)

        self._mariadb = MariaDbConnector(config)

    #
    # Protected inherited methods
    #
    def _insert_worker(self, worker):
        self._mariadb.execute_single_dml(self._QUERY['worker'], **worker)

    def _insert_input(self, inputs):
        self._mariadb.execute_single_dml(self._QUERY['input'], **inputs)

    def _insert_config(self, config):
        self._mariadb.execute_single_dml(self._QUERY['config'], **config)

    def _insert_task(self, task):
        self._mariadb.execute_single_dml(self._QUERY['task'], **task)

    def _insert_mission(self, mission):
        self._mariadb.execute_single_dml(self._QUERY['mission'], **mission)
