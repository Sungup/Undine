class UndineException(Exception):
    def __init__(self, *args, **_kwargs):
        """ Undine Exception Handler

        :param args: Exception arguments
        :param _kwargs: Currently not using.
        """
        Exception.__init__(self, *args)

    @property
    def message(self):
        return '\033[1m[Runtime Error]:\033[0m {}'.format(str(self))


class VirtualMethodException(UndineException):
    __MESSAGE = '{0} method is the virtual method of {1} class.'

    def __init__(self, class_type, method_name):
        UndineException.__init__(self,
                                 self.__MESSAGE.format(method_name,
                                                       class_type.__name__))
