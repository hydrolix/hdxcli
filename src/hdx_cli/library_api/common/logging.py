import sys
import logging

# key to have logging outputs without '\n' new line.
SPECIAL_CODE = '[!n]'


class InfoStreamHandler(logging.StreamHandler):
    def __init__(self, stream=None):
        super().__init__(stream or sys.stdout)

    def format(self, record):
        return record.msg

    def emit(self, record):
        if SPECIAL_CODE in record.msg:
            record.msg = record.msg.replace(SPECIAL_CODE, '')
            self.terminator = ''
        else:
            self.terminator = '\n'

        return super().emit(record)


class DebugStreamHandler(logging.StreamHandler):
    def __init__(self, stream=None):
        super().__init__(stream or sys.stdout)

    def format(self, record):
        formatter = logging.Formatter('%(asctime)s :: %(levelname)s :: %(message)s')
        return formatter.format(record)

    def emit(self, record):
        if SPECIAL_CODE in record.msg:
            record.msg = record.msg.replace(SPECIAL_CODE, '')
            self.terminator = ''
        else:
            self.terminator = '\n'

        return super().emit(record)


def set_debug_logger():
    set_logger(logging.DEBUG, DebugStreamHandler())


def set_info_logger():
    set_logger(logging.INFO, InfoStreamHandler())


def set_logger(level, handler: logging.StreamHandler):
    logger = logging.getLogger()
    logger.addHandler(handler)
    logger.setLevel(level)


def get_logger():
    return logging.getLogger()
