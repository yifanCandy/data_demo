import logging

class FileUtil:
    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger('data_migration')
        self.logger.info("init class FileUtil.")