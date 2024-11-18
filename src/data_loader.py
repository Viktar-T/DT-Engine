import os
import pandas as pd
from multiprocessing import Pool
import logging
from typing import List, Union
from src.config import RAW_DATA_DIR

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataLoader:
    """
    A class to handle loading raw data from various formats.
    Supports CSV and Excel files and includes basic validation checks.
    """

    def __init__(self, raw_data_path: str = RAW_DATA_DIR):
        """
        Initialize the DataLoader.

        Parameters:
        - raw_data_path: Path to the directory containing raw data files.
        """
        self.raw_data_path = raw_data_path
        if not os.path.exists(self.raw_data_path):
            logger.error(f"Directory '{self.raw_data_path}' does not exist.")
            raise FileNotFoundError(f"Directory '{self.raw_data_path}' does not exist.")
        logger.info(f"DataLoader initialized with raw data path: {self.raw_data_path}")
    
    def list_files(self, extensions: List[str] = ["csv", "xlsx"]) -> List[str]:
        """
        List all files in the raw data directory with the specified extensions.

        Parameters:
        - extensions: List of file extensions to filter by (e.g., ['csv', 'xlsx']).

        Returns:
        - List of file paths matching the extensions.
        """
        files = [
            os.path.join(self.raw_data_path, f)
            for f in os.listdir(self.raw_data_path)
            if os.path.isfile(os.path.join(self.raw_data_path, f)) and
            any(f.endswith(ext) for ext in extensions)
        ]
        logger.info(f"Found {len(files)} files with extensions {extensions} in '{self.raw_data_path}'")
        return files

    def load_data(self, file_name: str) -> Union[pd.DataFrame, None]:
        """
        Load data from a specified file.

        Parameters:
        - file_name: Name of the file to load data from.

        Returns:
        - DataFrame containing the loaded data, or None if the file format is unsupported.
        """
        file_path = os.path.join(self.raw_data_path, file_name)
        if not os.path.exists(file_path):
            logger.error(f"File '{file_path}' does not exist.")
            raise FileNotFoundError(f"File '{file_path}' does not exist.")
        
        logger.info(f"Loading data from file: {file_path}")
        if file_name.endswith('.csv'):
            data = pd.read_csv(file_path)
        elif file_name.endswith('.xlsx'):
            data = pd.read_excel(file_path)
        else:
            logger.error(f"Unsupported file format: {file_name}")
            return None
        
        logger.info(f"Data loaded successfully from file: {file_path}")
        return data

    def load_file(self, file_path: str) -> pd.DataFrame:
        """
        Load a single file into a pandas DataFrame.

        Parameters:
        - file_path: Path to the file to load.

        Returns:
        - DataFrame containing the file's data.
        """
        if not os.path.exists(file_path):
            logger.error(f"File '{file_path}' not found.")
            raise FileNotFoundError(f"File '{file_path}' not found.")

        file_extension = os.path.splitext(file_path)[1].lower()

        if file_extension == ".csv":
            data = pd.read_csv(file_path)
        elif file_extension in [".xls", ".xlsx"]:
            data = pd.read_excel(file_path)
        else:
            logger.error(f"Unsupported file format: {file_extension}")
            raise ValueError(f"Unsupported file format: {file_extension}")
        
        # Validate that the DataFrame is not empty
        if data.empty:
            logger.warning(f"File '{file_path}' is empty or contains no data.")
            raise ValueError(f"File '{file_path}' is empty or contains no data.")
        
        logger.info(f"File '{file_path}' loaded successfully.")
        return data
    
    def load_all_data(self) -> List[pd.DataFrame]:
        """
        Load all supported files from the raw data directory.

        Returns:
        - List of DataFrames, one for each successfully loaded file.
        """
        files = self.list_files()
        if not files:
            logger.error(f"No data files found in directory '{self.raw_data_path}'.")
            raise FileNotFoundError(f"No data files found in directory '{self.raw_data_path}'.")

        data_frames = []
        for file in files:
            try:
                df = self.load_file(file)
                data_frames.append(df)
                logger.info(f"Successfully loaded: {file}")
            except Exception as e:
                logger.error(f"Failed to load '{file}': {e}")
        
        return data_frames

    def save_to_parquet(self, data: pd.DataFrame, save_path: str):
        """
        Save a DataFrame to a Parquet file for efficient storage and retrieval.

        Parameters:
        - data: DataFrame to save.
        - save_path: Destination path for the Parquet file.
        """
        try:
            data.to_parquet(save_path, index=False)
            logger.info(f"Data successfully saved to: {save_path}")
        except Exception as e:
            logger.error(f"Failed to save data to '{save_path}': {e}")

    def get_metadata(self, data: pd.DataFrame) -> dict:
        """
        Extract metadata from a DataFrame.

        Parameters:
        - data: DataFrame to extract metadata from.

        Returns:
        - Dictionary containing metadata.
        """
        return {
            "rows": data.shape[0],
            "columns": data.shape[1],
            "size_in_memory": data.memory_usage(deep=True).sum(),
        }
    
    def parallel_load_files(self, file_names: List[str], num_workers: int = 4) -> List[pd.DataFrame]:
        """
        Load multiple files in parallel using multiprocessing.

        Parameters:
        - file_names: List of file names to load.
        - num_workers: Number of parallel processes.

        Returns:
        - List of DataFrames.
        """
        with Pool(num_workers) as pool:
            data_frames = pool.map(self.load_file, file_names)
        return data_frames
    
    def validate_data(self, data: pd.DataFrame) -> bool:
        """
        Perform basic validation checks on a DataFrame.

        Parameters:
        - data: DataFrame to validate.

        Returns:
        - True if validation passes, False otherwise.
        """
        if data.empty:
            logger.warning("Validation failed: DataFrame is empty.")
            return False
        if data.isnull().all(axis=None):
            logger.warning("Validation failed: DataFrame contains only NaN values.")
            return False
        return True
    
    


