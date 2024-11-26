import os
import logging
from datetime import datetime
from typing import List
from tabulate import tabulate  # Add this import

class LogManager:
    def __init__(self, logs_dir: str, 
                 names_of_files_under_procession: List[str] = None, 
                 metadata_manager=None):
        self.logs_dir = logs_dir
        self.names_of_files_under_procession = names_of_files_under_procession
        self.metadata_manager = metadata_manager
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
            # Create file handler with UTF-8 encoding
            fh = logging.FileHandler(log_file_path, encoding='utf-8')
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

    def split_dataframe(self, df, chunk_size):
        """
        Split a DataFrame into chunks of columns.

        Parameters:
        - df: The DataFrame to split.
        - chunk_size: The number of columns per chunk.

        Yields:
        - Chunks of the DataFrame.
        """
        for i in range(0, df.shape[1], chunk_size):
            yield df.iloc[:, i:i + chunk_size]

    def log_dataframe_in_chunks(self, df, file_name=None, chunk_size=6, rows=3):
        """
        Log a DataFrame in chunks to improve readability.

        Parameters:
        - df: The DataFrame to log.
        - file_name: The name of the file the data was loaded from (optional).
        - chunk_size: The number of columns per chunk.
        - rows: The number of rows to display per chunk.
        """
        for chunk in self.split_dataframe(df, chunk_size):
            if file_name:
                self.logger.info(f"Data from file '{file_name}':\n{tabulate(chunk.head(rows), headers='keys', tablefmt='fancy_grid')}")
            else:
                self.logger.info(f"DataFrame chunk:\n{tabulate(chunk.head(rows), headers='keys', tablefmt='fancy_grid')}")
