from terminaltables import AsciiTable
from undine.client.database.wrapper_base import WrapperBase


class CliWrapper(WrapperBase):
    def __init__(self, connector):
        WrapperBase.__init__(self, connector)

    def mission_list(self, list_all=False):
        table = AsciiTable([self._HEADER['mission_list']]
                           + self._connector.mission_list(list_all))

        for column in range(3, 8):
            table.justify_columns[column] = 'right'

        return table.table

    def mission_info(self, **kwargs):
        return AsciiTable([self._HEADER['mission_info']]
                          + self._connector.mission_info(**kwargs)).table
