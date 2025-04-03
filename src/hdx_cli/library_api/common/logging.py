import logging
import sys

from tqdm import tqdm

logging.getLogger("urllib3").setLevel(logging.CRITICAL)

# key to have logging outputs without '\n' new line.
SPECIAL_CODE = "[!n]"
# key to use for input cases
INPUT_SPECIAL_CODE = "[!i]"


class InfoStreamHandler(logging.StreamHandler):
    def __init__(self, stream=None):
        super().__init__(stream or sys.stdout)

    def format(self, record):
        return record.msg

    def emit(self, record):
        if SPECIAL_CODE in record.msg or INPUT_SPECIAL_CODE in record.msg:
            record.msg = record.msg.replace(SPECIAL_CODE, "").replace(INPUT_SPECIAL_CODE, "")
            self.terminator = ""
        else:
            self.terminator = "\n"

        return super().emit(record)


class DebugStreamHandler(logging.StreamHandler):
    def __init__(self, stream=None):
        super().__init__(stream or sys.stdout)
        self.last_message_no_newline = False

    def format(self, record):
        formatter = logging.Formatter("%(asctime)s :: %(levelname)s :: %(message)s")
        return formatter.format(record)

    def emit(self, record):
        try:
            msg = self.format(record)
            if SPECIAL_CODE in record.msg:
                if self.last_message_no_newline:
                    msg = record.getMessage().replace(SPECIAL_CODE, "")
                else:
                    self.last_message_no_newline = True
                    msg = msg.replace(SPECIAL_CODE, "")
            elif INPUT_SPECIAL_CODE in record.msg:
                msg = msg.replace(INPUT_SPECIAL_CODE, "")
            else:
                if self.last_message_no_newline:
                    self.last_message_no_newline = False
                    msg = f"{record.getMessage()}\n"
                else:
                    msg = f"{msg}\n"

            # tqdm.write is used to write to the console without new line
            tqdm.write(msg, end="")
            self.flush()
        except Exception:
            self.handleError(record)


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
