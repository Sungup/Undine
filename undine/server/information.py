from os import path, unlink
from undine.utils.exception import UndineException
from undine.utils.path import Path


class ConfigInfo:
    __DEFAULT_CONFIG_EXT = 'json'
    __STRING_FORMAT = "{'cid':{0}, 'name':'{1}', 'path':'{2}'}"

    def __init__(self, **kwargs):
        self.__cid = kwargs['cid']
        self.__name = kwargs['name']

        dir_ = kwargs.setdefault('dir', '')
        ext_ = kwargs.setdefault('ext', self.__DEFAULT_CONFIG_EXT)

        self.__path = Path.gen_uuid_file_path(dir_, ext_)

        with open(self.__path, 'w') as f_out:
            f_out.write(kwargs.setdefault('config', ''))

    def __del__(self):
        unlink(self.__path)

    def __str__(self):
        return self.__STRING_FORMAT.format(self.__cid, 
                                           self.__name, self.__path)

    @property
    def cid(self):
        return self.__cid

    @property
    def name(self):
        return self.__name

    @property
    def path(self):
        return self.__path


class InputInfo:
    __FILE_NOT_EXIST = 'Input file not exist at {0}'
    __STRING_FORMAT = "{'iid':{0}, 'name':'{1}', 'items':'{2}'}"

    def __init__(self, **kwargs):
        self.__iid = kwargs['iid']
        self.__name = kwargs['name']

        if isinstance(kwargs['items'], str):
            self.__items = [item.strip()
                            for item in kwargs['items'].split(',')]
        else:
            self.__items = kwargs['items']

    def __str__(self):
        return self.__STRING_FORMAT.format(self.__iid,
                                           self.__name, self.__items)

    @property
    def iid(self):
        return self.__iid

    @property
    def name(self):
        return self.__name

    def append(self, file_path):
        self.__items.append(file_path)

    def extend(self, *args):
        self.__items.extend(args)

    def inputs(self, input_dir=None):
        dir_ = input_dir if input_dir else ''

        string = ''

        for item in self.__items:
            full_path = path.join(dir_, item)

            if input_dir is not None and not path.isfile(full_path):
                raise UndineException(self.__FILE_NOT_EXIST.format(full_path))

            string += '{0} '.format(full_path)

        return string


class WorkerInfo:
    __STRING_FORMAT = '''
        {'wid':{0}, 'dir':'{1}', 'cmd':'{2}', 'arguments':'{3}', 
        'file_input': '{4}'}
    '''

    def __init__(self, **kwargs):
        self.__wid = kwargs['wid']
        self.__path = kwargs.setdefault('dir', '')
        self.__cmd = kwargs['cmd']
        self.__arguments = kwargs['arguments']
        self.__file_input = bool(kwargs.setdefault('file_input', True))

    def __str__(self):
        return self.__STRING_FORMAT.format(self.__wid, self.__path, self.__cmd,
                                           self.__arguments)

    @property
    def wid(self):
        return self.__wid

    @property
    def cmd(self):
        return path.join(self.__path, self.__cmd)

    @property
    def name(self):
        return self.__cmd

    @property
    def argument(self):
        return self.__arguments

    @property
    def use_file_inputs(self):
        return self.__file_input


class TaskInfo:
    __STRING_FORMAT = '''
        {'tid':{0}, 'cid':{1}, 'iid':{2}, 'wid':{3}, 'reportable': {4}}"
    '''

    def __init__(self, **kwargs):
        self.__tid = kwargs['tid']
        self.__cid = kwargs['cid']
        self.__iid = kwargs['iid']
        self.__wid = kwargs['wid']
        self.__reportable = bool(kwargs.setdefault('reportable', True))

    def __str__(self):
        return self.__STRING_FORMAT.format(self.__tid, self.__cid,
                                           self.__iid, self.__wid,
                                           self.__reportable)

    @property
    def tid(self):
        return self.__tid

    @property
    def cid(self):
        return self.__cid

    @property
    def iid(self):
        return self.__iid

    @property
    def wid(self):
        return self.__wid

    @property
    def reportable(self):
        return self.__reportable
