from undine.utils.system import eprint

import os
import uuid


class Path:
    """ Path utilities for Undine
    """

    ''' Error description variables
    '''
    __dir_create_fail__ = "Couldn't make directory path({0}) to store {1}."
    __file_create_fail__ = "Couldn't create file({0})."

    @staticmethod
    def open_uuid_file(directory, ext):
        # 1. Get full random uuid filename
        filename = directory + str(uuid.uuid4()) + '.' + ext

        # 2. Make directory if not exist
        try:
            os.makedirs(directory, mode=0o700, exist_ok=True)

            fp = open(filename, mode='w')

            return fp

        except OSError:
            eprint(Path.__dir_create_fail__.format(directory, 'uuid file'))
            raise

        except IOError:
            eprint(Path.__file_create_fail__.format(filename))
            raise
