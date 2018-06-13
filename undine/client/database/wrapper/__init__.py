from undine.client.database.base_client import BaseClient


class BaseWrapper(BaseClient):
    _HEADER = {
        'single_item': ('Field', 'Value'),
        'mission_list': ('MID', 'Name', 'Email',
                         'Ready', 'Issued', 'Done', 'Canceled', 'Failed',
                         'Issued At'),
        'mission_info': ('MID', 'Name', 'Email', 'Description', 'Issued At'),
        'task_list': ('TID', 'Name', 'Host', 'IP', 'State',
                      'Issued', 'Updated', 'Reportable'),
        'task_info': ('TID', 'Name', 'Host', 'IP', 'State',
                      'MID', 'CID', 'IID', 'WID',
                      'Issued At', 'Updated At', 'Reportable',
                      'Result', 'Succeed At', 'Error', 'Failed At'),
        'config_info': ('CID', 'Name', 'Config', 'Issued At'),
        'input_info': ('IID', 'Name', 'Items', 'Issued At'),
        'worker_info': ('WID', 'Name', 'Command',
                        'Arguments', 'Worker Directory', 'Issued At'),
        'host_list': ('Name', 'IP', 'Issued', 'Canceled', 'Failed',
                      'Registered', 'Logged-in', 'Logged-out', 'State')
    }

    def __init__(self, connector):
        BaseClient.__init__(self)

        self._connector = connector

    @property
    def db_config(self):
        return self._connector.db_config

    def rpc_call(self, ip, command, *args, **kwargs):
        return self._connector.rpc_call(ip, command, *args, **kwargs)

    def _tid_list(self, **kwargs):
        return self._connector.inner_tid_list(**kwargs)

    def _cancel_task(self, *args):
        return self._connector.inner_cancel_task(*args)
