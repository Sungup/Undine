from undine.client.client_base import ClientBase

import mysql.connector as mariadb


class MariaDbClient(ClientBase):
    _DEFAULT_HOST = 'localhost'
    _DEFAULT_DATABASE = 'undine'
    _DEFAULT_USER = 'undine'
    _DEFAULT_PASSWD = 'password'

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
            VALUES(UNHEX(%(iid)s, %(name)s, %(items)s)
        ''',
        'config': '''
            INSERT INTO config(cid, name, config)
            VALUES (UNHEX(%(cid)s), %(name)s, %(config)s)
        '''
    }

    def __init__(self, rabbitmq, config):
        ClientBase.__init__(self, rabbitmq)

        db_config = {
            'host': config.setdefault('host', self._DEFAULT_HOST),
            'database': config.setdefault('database', self._DEFAULT_DATABASE),
            'user': config.setdefault('user', self._DEFAULT_USER),
            'passwd': config.setdefault('password', self._DEFAULT_PASSWD)
        }

        try:
            self._pool = MariaDBConnectionPool(pool_name=db_config['database'],
                                               **db_config)
 
        except mariadb.Error as error:
            raise UndineException('MariaDB connection failed: {}'.format(error))


    #
    # Private methods
    #
    def _execute_dml(self, query, params):
        conn = self._pool.get_connection()

        cursor = conn.cursor()
        cursor.execute(query, params)
        cursor.close()

        conn.commit()
        conn.close()

    #
    # Protected inherite methods
    #
    def _insert_worker(self, _worker):
        raise UndineException('This method is the abstract method of fetch')

    def _insert_input(self, _input):
        raise UndineException('This method is the abstract method of fetch')

    def _insert_config(self, _config):
        raise UndineException('This method is the abstract method of fetch')

    def _insert_task(self, _task):
        raise UndineException('This method is the abstract method of fetch')
