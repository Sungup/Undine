from undine import information as info
from undine.utils.exception import UndineException
from undine.utils.path import Path

import os


class Task:
    """
    Task object for each task command.

    Task object contains the task id, command line string, config file path
    and input file list. Task scheduler run each this task object and store
    the return message after task has done. If this task generate the result
    file, Task object collect the result data from the stored result file and
    unlink that.
    """

    _DEFAULT_SUCCESS_MESSAGE = 'Success'
    _DEFAULT_FAIL_MESSAGE = 'Fail'

    def __init__(self, tid, w_info, c_info, i_info, **kwargs):
        """
        Parameters

        :param int tid: Task id from TaskInfo

        :param w_info: WorkerInfo object which contains the task command
        :type w_info: undine.information.WorkerInfo

        :param c_info: ConfigInfo object which contains the task config
        :type c_info: undine.information.ConfigInfo

        :param i_info: InputInfo object which contains the list of input files
        :type i_info: undine.information.InputInfo

        Keyword Arguments

        :param result_dir: Result file home path
        :type result_dir: str

        :param input_dir: Input file home path
        :type input_dir: str

        :param ext: File extension
        :type ext: str
        """

        # Check the input argument type check
        # TODO How reduce lines!
        if not isinstance(w_info, info.WorkerInfo):
            raise UndineException('w_info is not WorkerInfo')

        if not isinstance(c_info, info.ConfigInfo):
            raise UndineException('c_info is not ConfigInfo')

        if not isinstance(i_info, info.InputInfo):
            raise UndineException('i_info is not InputInfo')

        self._tid = tid
        self._worker = w_info

        dir_ = kwargs.setdefault('result_dir', '')
        ext_ = kwargs.setdefault('ext', '.json')
        name_ = "{0}-{1}".format(c_info.name, i_info.name)

        self._config_path = c_info.path
        self._result_path = Path.gen_file_path(dir_, name_, ext_)
        self._input_list = i_info.inputs(kwargs.setdefault('input_dir', ''))

        # Result container
        self._message = None
        self._result = None

    @property
    def cmd(self):
        arg_string = self._worker.argument                          \
                                 .replace('%C', self._config_path)  \
                                 .replace('%R', self._result_path)  \
                                 .replace('%I', self._input_list)

        return "{0} {1}".format(self._worker.cmd, arg_string)

    @property
    def tid(self):
        return self._tid

    @property
    def message(self):
        return self._message

    @property
    def result(self):
        return self._result

    @property
    def is_success(self):
        return bool(self._result)

    def success(self, message = _DEFAULT_SUCCESS_MESSAGE):
        """
        Check done and load result context from result file.

        :param str message: Command line success message.
        :return: None
        """
        if os.path.exists(self._result_path):
            self._result = open(self._result_path, 'r').read()
            os.unlink(self._result_path)
        else:
            self._result = message

        self._message = message

    def fail(self, message = _DEFAULT_FAIL_MESSAGE):
        """
        Check fail

        :param str message: Command line error message.
        :return: None
        """
        self._message = message
