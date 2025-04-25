import logging


class ReportLog:
    """A tool to collect many log messages and report them all at once"""

    def __init__(self, logger_name: str = __name__) -> None:
        self.messages = {}
        self.logger = logging.getLogger(logger_name)

    def add_message(self, level: int, message: str) -> None:
        """Add a message to the report"""
        self.messages.setdefault(level, [])
        self.messages[level].append(message)

    @property
    def has_messages(self):
        """Does this report have messages?"""
        return bool(self.messages)

    def print_report(self) -> None:
        """Log all the messages in the report"""
        if self.has_messages:
            for loglevel, all_messages in self.messages.items():
                for message in all_messages:
                    level_name = logging.getLevelName(int(loglevel))
                    full_message = f"[{level_name}] {message}"
                    self.logger.log(loglevel, full_message)
