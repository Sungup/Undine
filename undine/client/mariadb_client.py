from undine.client.network_client_base import NetworkClientBase
from undine.database.mariadb import MariaDbConnector


class MariaDbClient(NetworkClientBase):
    _QUERY = {
        'task': '''
            INSERT INTO task(tid, name, cid, iid, wid)
            VALUES(UNHEX(%(tid)s), %(name)s,
                   UNHEX(%(cid)s), UNHEX(%(iid)s), UNHEX(%(wid)s))
        ''',
        'worker': '''
            INSERT INTO worker(wid, name, command, arguments, worker_dir)
            VALUES(UNHEX(%(wid)s), %(name)s, %(command)s,
                   %(arguments)s, %(worker_dir)s)
        ''',
        'input': '''
            INSERT INTO input(iid, name, items)
            VALUES(UNHEX(%(iid)s), %(name)s, %(items)s)
        ''',
        'config': '''
            INSERT INTO config(cid, name, config)
            VALUES (UNHEX(%(cid)s), %(name)s, %(config)s)
        '''
    }

    def __init__(self, rabbitmq, config):
        NetworkClientBase.__init__(self, rabbitmq)

        self._mariadb = MariaDbConnector(config)

    #
    # Protected inherited methods
    #
    def _insert_worker(self, worker):
        self._mariadb.execute_single_dml(self._QUERY['worker'], worker)

    def _insert_input(self, inputs):
        self._mariadb.execute_single_dml(self._QUERY['input'], inputs)

    def _insert_config(self, config):
        self._mariadb.execute_single_dml(self._QUERY['config'], config)

    def _insert_task(self, task):
        self._mariadb.execute_single_dml(self._QUERY['task'], task)
