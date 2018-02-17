from undine.client.database.client_base import ClientBase


class WrapperBase(ClientBase):
    _HEADER = {
        'single_item': ('Field', 'Value'),
        'mission_list': ('MID', 'Name', 'Email',
                         'Ready', 'Issued', 'Done', 'Canceled', 'Failed',
                         'Issued At'),
        'mission_info': ('MID', 'Name', 'Email', 'Description', 'Issued At'),
        'task_list': ('TID', 'Name', 'Host', 'IP', 'State',
                      'Issued', 'Updated', 'Reportable'),
        'task_info': ('TID', 'Name', 'Host', 'IP', 'State', 'CID', 'IID', 'WID',
                      'Issued At', 'Updated At', 'Reportable',
                      'Result', 'Succeed At', 'Error', 'Failed At')
    }

    def __init__(self, connector):
        self._connector = connector
