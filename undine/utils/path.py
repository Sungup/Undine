from undine.utils.system import eprint

import os
import uuid


class Path:
    """ Path utilities for Undine
    """

    ''' Error description variables
    '''
    _DIR_CREATE_FAIL = "Couldn't make directory path({0}) to store {1}."
    _FILE_CREATE_FAIL = "Couldn't create file({0})."

    @staticmethod
    def gen_file_path(directory, name, ext):
        # 1. Get full random uuid filename
        filename = os.path.join(directory, name + ext)

        # 2. Make directory if not exist
        try:
            os.makedirs(directory, mode=0o700, exist_ok=True)

        except OSError:
            eprint(Path._DIR_CREATE_FAIL.format(directory, 'uuid file'))
            raise

        return filename

    @staticmethod
    def open_file(directory, name, ext):
        filename = Path.gen_file_path(directory, name, ext)

        try:
            fp = open(filename, mode='w')

            return fp

        except IOError:
            eprint(Path._FILE_CREATE_FAIL.format(filename))
            raise

    @staticmethod
    def gen_uuid_file_path(directory, ext):
        return Path.gen_file_path(directory, str(uuid.uuid4()), ext)

    @staticmethod
    def open_uuid_file(directory, ext):
        filename = Path.gen_uuid_file_path(directory, ext)

        try:
            fp = open(filename, mode='w')

            return fp

        except IOError:
            eprint(Path._FILE_CREATE_FAIL.format(filename))
            raise
