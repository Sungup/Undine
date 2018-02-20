from undine.server.information import ConfigInfo, InputInfo
from undine.server.driver.file import FileDriver

import json


class JSONDriver(FileDriver):
    #
    # Private/Protected method
    #
    def _load_config(self, file_path):
        self._configs = dict()

        cid = 0
        config_obj = json.load(open(file_path, 'r'))

        for name, data in config_obj.items():
            config = json.dumps(data, indent=4)
            self._configs[cid] = ConfigInfo(cid=cid, name=name, config=config,
                                            dir=self._config_dir,
                                            ext=self._config_ext)

            cid += 1

    def _load_inputs(self, file_path):
        self._inputs = dict()

        iid = 0
        input_obj = json.load(open(file_path, 'r'))

        for name, items in input_obj.items():
            self._inputs[iid] = InputInfo(iid=iid, name=name, items=items)

            iid += 1

    def __init__(self, config, config_dir):
        FileDriver.__init__(self, config, config_dir)
