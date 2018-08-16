from terminaltables import AsciiTable
from undine.client.view.base_view import BaseView


#
# Simple by viewer
#
class TableView(BaseView):
    def __init__(self, connector):
        super(TableView, self).__init__(connector)

    def __single_table(self, header, data):
        return AsciiTable([self._HEADER['single_item']]
                          + list(zip(header, data)))

    #
    # Abstract method
    #
    def mission_list(self, list_all=False):
        table = AsciiTable([self._HEADER['mission_list']]
                           + self._connector.mission_list(list_all))

        for column in range(3, 8):
            table.justify_columns[column] = 'right'

        return table.table

    def mission_info(self, **kwargs):
        data = self._connector.mission_info(**kwargs)

        if len(data) == 1:
            return self.__single_table(self._HEADER['mission_info'],
                                       *data).table
        elif data:
            return AsciiTable([self._HEADER['mission_info']] + data).table
        else:
            return AsciiTable([('Mission information is not exist.', )]).table

    def task_list(self, **kwargs):
        return AsciiTable([self._HEADER['task_list']]
                          + self._connector.task_list(**kwargs)).table

    def task_info(self, tid):
        return self.__single_table(self._HEADER['task_info'],
                                   self._connector.task_info(tid)).table

    def config_info(self, cid):
        return self.__single_table(self._HEADER['config_info'],
                                   self._connector.config_info(cid)).table

    def input_info(self, iid):
        return self.__single_table(self._HEADER['input_info'],
                                   self._connector.input_info(iid)).table

    def input_list(self):
        return AsciiTable([self._HEADER['input_info']]
                          + self._connector.input_list()).table

    def worker_info(self, wid):
        return self.__single_table(self._HEADER['worker_info'],
                                   self._connector.worker_info(wid)).table

    def worker_list(self):
        return AsciiTable([self._HEADER['worker_info']]
                          + self._connector.worker_list()).table

    def host_list(self):
        table = AsciiTable([self._HEADER['host_list']]
                           + self._connector.host_list())

        for column in range(2, 5):
            table.justify_columns[column] = 'right'

        return table.table
