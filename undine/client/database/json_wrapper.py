from undine.client.database.wrapper_base import WrapperBase

import json


class JsonWrapper(WrapperBase):
    def __init__(self, connector):
        WrapperBase.__init__(self, connector)

    @staticmethod
    def export(header, values):
        return json.dumps([dict(zip([name.replace(' ', '_').lower()
                                     for name in header], item))
                           for item in values], indent=2)

    def mission_list(self, list_all=False):
        return JsonWrapper.export(self._HEADER['mission_list'],
                                  self._connector.mission_list(list_all))

    def mission_info(self, **kwargs):
        return JsonWrapper.export(self._HEADER['mission_info'],
                                  self._connector.mission_info(**kwargs))
