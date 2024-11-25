import os
import logging
from datetime import datetime

class LogManager:
    def __init__(self, logs_dir: str):
        self.logs_dir = logs_dir
        self._setup_logging()  # Call the setup logging method

    def _get_log_file_path(self) -> str:
        date_time_str = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        return os.path.join(self.logs_dir, f'log_{date_time_str}.log')

    def _setup_logging(self):
        if not os.path.exists(self.logs_dir):
            os.makedirs(self.logs_dir)
        log_file_path = self._get_log_file_path()
        self.logger = logging.getLogger('LogManager')
        self.logger.setLevel(logging.INFO)
        # Check if the logger already has handlers to avoid duplicate logs
        if not self.logger.handlers:
            # Create file handler
            fh = logging.FileHandler(log_file_path)
            fh.setLevel(logging.INFO)
            # Create formatter
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            fh.setFormatter(formatter)
            # Add handler to logger
            self.logger.addHandler(fh)

    def log_info(self, message: str):
        self.logger.info(message)

    def log_error(self, message: str):
        self.logger.error(message)

    def log_warning(self, message: str):
        self.logger.warning(message)

    def log_debug(self, message: str):
        self.logger.debug(message)
