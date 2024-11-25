import os
import logging
from datetime import datetime

class LogManager:
    def __init__(self, logs_dir: str):
        self.logs_dir = logs_dir
        self.log_file = self._get_log_file_path()
        self._setup_logging()

    def _get_log_file_path(self) -> str:
        date_str = datetime.now().strftime('%Y-%m-%d')
        return os.path.join(self.logs_dir, f'log_{date_str}.log')

    def _setup_logging(self):
        if not os.path.exists(self.logs_dir):
            os.makedirs(self.logs_dir)
        logging.basicConfig(
            filename=self.log_file,
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def log_info(self, message: str):
        self.logger.info(message)

    def log_error(self, message: str):
        self.logger.error(message)

    def log_warning(self, message: str):
        self.logger.warning(message)

    def log_debug(self, message: str):
        self.logger.debug(message)
