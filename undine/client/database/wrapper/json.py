from undine.client.database.wrapper import BaseWrapper

import json


class JsonWrapper(BaseWrapper):
    def __init__(self, connector):
        BaseWrapper.__init__(self, connector)

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

    def config_info(self, cid):
        return JsonWrapper._export_item(self._HEADER['config_info'],
                                        self._connector.config_info(cid))

    def input_info(self, iid):
        return JsonWrapper._export_item(self._HEADER['input_info'],
                                        self._connector.input_info(iid))

    def input_list(self):
        return JsonWrapper._export_list(self._HEADER['input_info'],
                                        self._connector.input_list())

    def worker_info(self, wid):
        return JsonWrapper._export_item(self._HEADER['worker_info'],
                                        self._connector.worker_info(wid))

    def worker_list(self):
        return JsonWrapper._export_list(self._HEADER['worker_info'],
                                        self._connector.worker_list())

    def host_list(self):
        return JsonWrapper._export_list(self._HEADER['host_list'],
                                        self._connector.host_list())
