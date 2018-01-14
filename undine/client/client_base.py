from undine.utils.exception import UndineException

import json
import uuid


class ClientBase:
    @staticmethod
    def _get_uuid():
        return str(uuid.uuid4()).replace('-', '')

    #
    # Public methods
    #
    def publish_worker(self, name, command, arguments, worker_dir):
        worker = {
            'wid': self._get_uuid(),
            'name': name,
            'command': command,
            'arguments': arguments,
            'worker_dir': worker_dir
        }

        self._insert_worker(worker)

        return worker['wid']

    def publish_input(self, name, items):
        inputs = {
            'iid': self._get_uuid(),
            'name': name,
            'items': items if isinstance(items, str) else ','.join(items)
        }

        self._insert_input(inputs)

        return inputs['iid']

    def publish_config(self, name, content):
        config = {
            'cid': self._get_uuid(),
            'name': name,
            'config': json.dumps(content)
        }

        self._insert_config(config)

        return config['cid']

    def publish_task(self, name, cid, iid, wid):
        task = {
            'tid': self._get_uuid(),
            'name': name,
            'cid': cid,
            'iid': iid,
            'wid': wid
        }

        # Insert into remote host
        self._insert_task(task)

        return task['tid']

    #
    # Protected inherited methods
    #
    def _insert_worker(self, _worker):
        raise UndineException('This method is the abstract method of fetch')

    def _insert_input(self, _inputs):
        raise UndineException('This method is the abstract method of fetch')

    def _insert_config(self, _config):
        raise UndineException('This method is the abstract method of fetch')

    def _insert_task(self, _task):
        raise UndineException('This method is the abstract method of fetch')
