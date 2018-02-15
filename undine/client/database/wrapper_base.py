from undine.client.database.client_base import ClientBase


class WrapperBase(ClientBase):
    _HEADER = {
        'mission_list': ('MID', 'Name', 'Email',
                         'Ready', 'Issued', 'Done', 'Canceled', 'Failed',
                         'Issued At'),
        'mission_info': ('MID', 'Name', 'Email', 'Description', 'Issued At')
    }

    def __init__(self, connector):
        self._connector = connector
