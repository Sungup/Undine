from undine.api.database.base_client import BaseClient
from undine.database.sqlite import SQLiteConnector


class SQLiteClient(BaseClient):
    _QUERY = {
        'task': '''
            INSERT INTO task(tid, name, cid, iid, wid, reportable)
            VALUES(:tid, :name, :cid, :iid, :wid, :reportable)
        ''',
        'worker': '''
            INSERT INTO worker(wid, name, command, arguments, worker_dir)
            VALUES(:wid, :name, :command, :arguments, :worker_dir)
        ''',
        'input': '''
            INSERT INTO input(iid, name, items)
            VALUES(:iid, :name, :items)
        ''',
        'config': '''
            INSERT INTO config(cid, name, config)
            VALUES (:cid, :name, :config)
        '''
    }

    def __init__(self, config):
        BaseClient.__init__(self)

        self._sqlite = SQLiteConnector(config)

    #
    # Protected inherited methods
    #
    def _insert_worker(self, worker):
        self._sqlite.execute_single_dml(self._QUERY['worker'], worker)

    def _insert_input(self, inputs):
        self._sqlite.execute_single_dml(self._QUERY['input'], inputs)

    def _insert_config(self, config):
        self._sqlite.execute_single_dml(self._QUERY['config'], config)

    def _insert_task(self, task):
        self._sqlite.execute_single_dml(self._QUERY['task'], task)
