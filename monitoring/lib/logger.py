import logging


class Logger:
    def __init__(self, file_name, stream_handler=True, file_handler=True):
        self.logger = logging.getLogger(file_name)
        self.logger.setLevel(logging.INFO)
        if stream_handler:
            sh = logging.StreamHandler()
            self.logger.addHandler(sh)
        if file_handler:
            fh = logging.FileHandler(file_name)
            self.logger.addHandler(fh)
