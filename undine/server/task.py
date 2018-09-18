from undine.server import information as info
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

    __DEFAULT_SUCCESS_MESSAGE = 'Success'
    __DEFAULT_FAIL_MESSAGE = 'Fail'

    def __init__(self, t_info, w_info, c_info, i_info, **kwargs):
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

        self.__task = t_info
        self.__worker = w_info

        dir_ = kwargs.setdefault('result_dir', '')
        ext_ = kwargs.setdefault('result_ext', '.json')

        self.__config_path = c_info.path
        self.__result_path = Path.gen_file_path(dir_, t_info.tid, ext_)

        if self.__worker.use_file_inputs:
            input_dir = kwargs.setdefault('input_dir', None)
        else:
            input_dir = None

        self.__input_list = i_info.inputs(input_dir)

        # Result container
        self.__message = None
        self.__result = None

    @property
    def cmd(self):
        arg_string = self.__worker.argument                           \
                                  .replace('%C', self.__config_path)  \
                                  .replace('%R', self.__result_path)  \
                                  .replace('%I', self.__input_list)

        return "{0} {1}".format(self.__worker.cmd, arg_string)

    @property
    def tid(self):
        return self.__task.tid

    @property
    def report(self):
        return self.__task.reportable

    @property
    def message(self):
        return self.__message

    @property
    def result(self):
        return self.__result

    @property
    def is_success(self):
        return bool(self.__result)

    def success(self, message = __DEFAULT_SUCCESS_MESSAGE):
        """
        Check done and load result context from result file.

        :param str message: Command line success message.
        :return: None
        """
        if os.path.exists(self.__result_path):
            self.__result = open(self.__result_path, 'r').read()
            os.unlink(self.__result_path)
        else:
            self.__result = message

        self.__message = message

    def fail(self, message = __DEFAULT_FAIL_MESSAGE):
        """
        Check fail

        :param str message: Command line error message.
        :return: None
        """
        self.__message = message
