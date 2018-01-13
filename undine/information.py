from os import path, unlink
from undine.utils.exception import UndineException
from undine.utils.path import Path


class ConfigInfo:
    _DEFAULT_CONFIG_EXT = 'json'
    _STRING_FORMAT = "{'cid':{0}, 'name':'{1}', 'path':'{2}'}"

    def __init__(self, **kwargs):
        self._cid = kwargs['cid']
        self._name = kwargs['name']

        dir_ = kwargs.setdefault('dir', '')
        ext_ = kwargs.setdefault('ext', self._DEFAULT_CONFIG_EXT)

        self._path = Path.gen_uuid_file_path(dir_, ext_)

        with open(self._path, 'w') as f_out:
            f_out.write(kwargs.setdefault('config', ''))

    def __del__(self):
        unlink(self._path)

    def __str__(self):
        return self._STRING_FORMAT.format(self._cid, self._name, self._path)

    @property
    def cid(self):
        return self._cid

    @property
    def name(self):
        return self._name

    @property
    def path(self):
        return self._path


class InputInfo:
    _FILE_NOT_EXIST = 'Input file not exist at {0}'
    _STRING_FORMAT = "{'iid':{0}, 'name':'{1}', 'items':'{2}'}"

    def __init__(self, **kwargs):
        self._iid = kwargs['iid']
        self._name = kwargs['name']

        if isinstance(kwargs['items'], str):
            self._items = [item.strip() for item in kwargs['items'].split(',')]
        else:
            self._items = kwargs['items']

    def __str__(self):
        return self._STRING_FORMAT.format(self._iid, self._name, self._items)

    @property
    def iid(self):
        return self._iid

    @property
    def name(self):
        return self._name

    def append(self, file_path):
        self._items.append(file_path)

    def extend(self, *args):
        self._items.extend(args)

    def inputs(self, input_dir=None):
        dir_ = input_dir if input_dir else ''

        string = ''

        for item in self._items:
            full_path = path.join(dir_, item)

            if not path.isfile(full_path):
                raise UndineException(self._FILE_NOT_EXIST.format(full_path))

            string += '{0} '.format(full_path)

        return string


class WorkerInfo:
    _STRING_FORMAT = "{'wid':{0}, 'dir':'{1}', 'cmd':'{2}', 'arguments':'{3}'}"

    def __init__(self, **kwargs):
        self._wid = kwargs['wid']
        self._path = kwargs.setdefault('dir', '')
        self._cmd = kwargs['cmd']
        self._arguments = kwargs['arguments']

    def __str__(self):
        return self._STRING_FORMAT.format(self._wid, self._path, self._cmd,
                                          self._arguments)

    @property
    def wid(self):
        return self._wid

    @property
    def cmd(self):
        return path.join(self._path, self._cmd)

    @property
    def name(self):
        return self._cmd

    @property
    def argument(self):
        return self._arguments


class TaskInfo:
    _STRING_FORMAT = "{'tid':{0}, 'cid':{1}, 'iid':{2}, 'wid':{3}}"

    def __init__(self, **kwargs):
        self._tid = kwargs['tid']
        self._cid = kwargs['cid']
        self._iid = kwargs['iid']
        self._wid = kwargs['wid']

    def __str__(self):
        return self._STRING_FORMAT.format(self._tid, self._cid,
                                          self._iid, self._wid)

    @property
    def tid(self):
        return self._tid

    @property
    def cid(self):
        return self._cid

    @property
    def iid(self):
        return self._iid

    @property
    def wid(self):
        return self._wid
