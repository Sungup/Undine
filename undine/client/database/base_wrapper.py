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
                        'Arguments', 'Worker Directory', 'Issued At')
    }

    def __init__(self, connector):
        self._connector = connector
