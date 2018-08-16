from undine.api.database.base_client import BaseNetworkClient
from undine.database.mariadb import MariaDbConnector


class MariaDbClient(BaseNetworkClient):
    _QUERY = {
        'mission': '''
            INSERT INTO mission(mid, name, email, description)
            VALUES(%(mid)s, %(name)s, %(email)s, %(description)s)
        ''',
        'task': '''
            INSERT INTO task(tid, name, cid, iid, wid, mid, reportable)
            VALUES(%(tid)s, %(name)s,
                   %(cid)s, %(iid)s,
                   %(wid)s, %(mid)s,
                   %(reportable)s)
        ''',
        'worker': '''
            INSERT INTO worker(wid, name, command, arguments, worker_dir)
            VALUES(%(wid)s, %(name)s, %(command)s,
                   %(arguments)s, %(worker_dir)s)
        ''',
        'input': '''
            INSERT INTO input(iid, name, items)
            VALUES(%(iid)s, %(name)s, %(items)s)
        ''',
        'config': '''
            INSERT INTO config(cid, name, config)
            VALUES (%(cid)s, %(name)s, %(config)s)
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
