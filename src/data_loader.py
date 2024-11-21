import os
import pandas as pd
from multiprocessing import Pool
import logging
import json
from typing import List, Union
from src.config import RAW_DATA_DIR
from tabulate import tabulate

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def split_dataframe(df, chunk_size):
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

def log_dataframe_in_chunks(df, file_name, chunk_size=6, rows=3):
    """
    Log a DataFrame in chunks to improve readability.

    Parameters:
    - df: The DataFrame to log.
    - file_name: The name of the file the data was loaded from.
    - chunk_size: The number of columns per chunk.
    - rows: The number of rows to display per chunk.
    """
    for chunk in split_dataframe(df, chunk_size):
        logger.info(f"Data from file '{file_name}':\n{tabulate(chunk.head(rows), headers='keys', tablefmt='fancy_grid')}")

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

    # !!!! I have files_with_raw_data_links.json. Don't used in main.py !!!!    
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
    
    # !!!! used in main.py !!!!
    def select_from_json_and_load_data(self, selected_id: int) -> List[pd.DataFrame]:
        """
        Select data files to load based on 'files_with_raw_data_links.json' and a provided 'id'.

        Parameters:
        - selected_id: The ID of the data entry to load.

        Returns:
        - List of DataFrames loaded from the selected files.
        """
        # Define the path to the JSON file
        json_file_path = os.path.join(self.raw_data_path, 'files_with_raw_data_links.json')

        # Check if the JSON file exists
        if not os.path.exists(json_file_path):
            logger.error(f"JSON file '{json_file_path}' does not exist.")
            raise FileNotFoundError(f"JSON file '{json_file_path}' does not exist.")

        # Open and read the JSON file
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data_links = json.load(f)

        # Flatten the JSON structure to a list of entries
        entries = []
        for category, items in data_links.items():
            for item in items:
                item['category'] = category
                entries.append(item)

        # Find the entry with the selected ID
        selected_entry = next((entry for entry in entries if entry['id'] == selected_id), None)

        if not selected_entry:
            logger.error(f"No entry found with ID {selected_id}.")
            raise ValueError(f"No entry found with ID {selected_id}.")

        # Get the main_file_name and eco_file_name
        main_file_name = selected_entry.get('main_file_name', '')
        eco_file_name = selected_entry.get('eco_file_name', '')

        data_frames = []

        # Load the main file using the existing load_data method
        if main_file_name:
            logger.info(f"Loading main file: {main_file_name}")
            df_main = self.load_data(main_file_name)
            if df_main is not None:
                data_frames.append(df_main)
            else:
                logger.error(f"Failed to load main file '{main_file_name}'.")
                raise FileNotFoundError(f"Main file '{main_file_name}' not found or failed to load.")

        # Load the eco file using the existing load_data method
        if eco_file_name:
            logger.info(f"Loading eco file: {eco_file_name}")
            df_eco = self.load_data(eco_file_name)
            if df_eco is not None:
                data_frames.append(df_eco)
            else:
                logger.error(f"Failed to load eco file '{eco_file_name}'.")
                raise FileNotFoundError(f"Eco file '{eco_file_name}' not found or failed to load.")

        logger.info("Data files loaded successfully.")
        return data_frames


    # !!!! used in def select_from_json_and_load_data(self, selected_id: int)  !!!!
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
        # logger.info(f"Data from file '{file_name}':\n{tabulate(data.head(), headers='keys', tablefmt='fancy_grid')}")
        log_dataframe_in_chunks(data, file_name)

        logger.info(f"Data Frame Shape: {data.shape}")
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


