class UndineException(Exception):
    def __init__(self, *args, **_kwargs):
        """ Undine Exception Handler

        :param args: Exception arguments
        :param _kwargs: Currently not using.
        """
        Exception.__init__(self, *args)
