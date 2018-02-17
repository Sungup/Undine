from undine.client.database.wrapper_base import WrapperBase

import json


class JsonWrapper(WrapperBase):
    def __init__(self, connector):
        WrapperBase.__init__(self, connector)

    @staticmethod
    def _export_list(header, values):
        return json.dumps([dict(zip([name.replace(' ', '_').lower()
                                     for name in header], item))
                           for item in values], indent=2)

    @staticmethod
    def _export_item(header, value):
        print(value)
        return json.dumps(dict(zip([name.replace(' ', '_').lower()
                                    for name in header], value)), indent=2)

    def mission_list(self, list_all=False):
        return JsonWrapper._export_list(self._HEADER['mission_list'],
                                        self._connector.mission_list(list_all))

    def mission_info(self, **kwargs):
        return JsonWrapper._export_list(self._HEADER['mission_info'],
                                        self._connector.mission_info(**kwargs))

    def task_list(self, **kwargs):
        return JsonWrapper._export_list(self._HEADER['task_list'],
                                        self._connector.task_list(**kwargs))

    def task_info(self, tid):
        return JsonWrapper._export_item(self._HEADER['task_info'],
                                        self._connector.task_info(tid))
