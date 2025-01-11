import os
import pandas as pd
from multiprocessing import Pool
import logging
import json
from typing import List, Union
from src.config import RAW_DATA_DIR
from src.metadata_manager import MetadataManager
from src.log_manager import LogManager
from ftfy import fix_text, fix_encoding
import chardet

# Set up logging
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# logger = logging.getLogger(__name__)

class DataLoader:
    """
    A class to handle loading raw data from various formats.
    Supports CSV and Excel files and includes basic validation checks.
    """

    def __init__(self, raw_data_path: str = None, 
                 names_of_files_under_procession: List[str] = None,
                 json_path: str = None,
                 metadata_manager: MetadataManager = None, 
                 log_manager: LogManager = None):
        """
        Initialize the DataLoader.

        Parameters:
        - raw_data_path: Path to the directory containing raw data files.
        - metadata_manager: An instance of MetadataManager to handle metadata.
        - log_manager: An instance of LogManager for logging.
        """
        self.raw_data_path = raw_data_path
        self.names_of_files_under_procession = names_of_files_under_procession
        self.json_path = json_path
        if not os.path.exists(self.raw_data_path):
            if log_manager:
                log_manager.log_error(f"Directory '{self.raw_data_path}' does not exist.")
            raise FileNotFoundError(f"Directory '{self.raw_data_path}' does not exist.")
        self.metadata_manager = metadata_manager
        self.log_manager = log_manager
        if self.log_manager:
            self.log_manager.log_info(f"DataLoader initialized with raw data path: {self.raw_data_path}")

    # !!!! I have files_with_raw_data_links.json. Don't used in main.py !!!!    
    def list_files(self, extensions: List[str] = ["csv", "xlsx", "parquet"]) -> List[str]:
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
        if self.log_manager:
            self.log_manager.log_info(f"Found {len(files)} files with extensions {extensions} in '{self.raw_data_path}'")
        return files
    
    # !!!! used in main.py !!!!
    def select_from_json_and_load_data(self, selected_id: int) -> List[pd.DataFrame]:
        """
        Select data files to load based on 'self.json_path.json' and a provided 'id'.

        Parameters:
        - selected_id: The ID of the data entry to load.

        Returns:
        - List of DataFrames loaded from the selected files.
        """

        # Check if the JSON file exists
        if not os.path.exists(self.json_path):
            if self.log_manager:
                self.log_manager.log_error(f"JSON file '{self.json_path}' does not exist.")
            raise FileNotFoundError(f"JSON file '{self.json_path}' does not exist.")

        # Open and read the JSON file
        with open(self.json_path, 'r', encoding='utf-8') as f:
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
            if self.log_manager:
                self.log_manager.log_error(f"No entry found with ID {selected_id}.")
            raise ValueError(f"No entry found with ID {selected_id}.")

        # Get the main_file_name and eco_file_name
        main_file_name = selected_entry.get('main_file_name', '')
        eco_file_name = selected_entry.get('eco_file_name', '')

        data_frames = []

        # Load the main file using the existing load_data method
        if main_file_name:
            if self.log_manager:
                self.log_manager.log_info(f"Loading main file: {main_file_name}")
            df_main = self.load_data(main_file_name)
            if df_main is not None:
                data_frames.append(df_main)
            else:
                if self.log_manager:
                    self.log_manager.log_error(f"Failed to load main file '{main_file_name}'.")
                raise FileNotFoundError(f"Main file '{main_file_name}' not found or failed to load.")

        # Load the eco file using the existing load_data method
        #if eco_file_name:
        #    if self.log_manager:
        #        self.log_manager.log_info(f"Loading eco file: {eco_file_name}")
        #    df_eco = self.load_data(eco_file_name)
        #    if df_eco is not None:
        #        data_frames.append(df_eco)
        #    else:
        #        if self.log_manager:
        #            self.log_manager.log_error(f"Failed to load eco file '{eco_file_name}'.")
        #        raise FileNotFoundError(f"Eco file '{eco_file_name}' not found or failed to load.")

        if self.log_manager:
            self.log_manager.log_info("Data files loaded successfully.")
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
        if file_name == "empty":
            return None
        
        file_path = os.path.join(self.raw_data_path, file_name)
        if not os.path.exists(file_path):
            if self.log_manager:
                self.log_manager.log_error(f"File '{file_path}' does not exist.")
            raise FileNotFoundError(f"File '{file_path}' does not exist.")
        
        if self.log_manager:
            self.log_manager.log_info(f"Loading data from file: {file_path}")
        if file_name.endswith('.csv'):
            data = pd.read_csv(file_path)
        elif file_name.endswith('.xlsx'):
            data = pd.read_excel(file_path)
        elif file_name.endswith('.parquet'):
            data = pd.read_parquet(file_path)
            data.columns = [fix_encoding(col) for col in data.columns]
        else:
            if self.log_manager:
                self.log_manager.log_error(f"Unsupported file format: {file_name}")
            return None
        
        if self.log_manager:
            self.log_manager.log_info(f"Data loaded successfully from file: {file_path}")
            self.log_manager.log_info(f"Data Frame Columns: {list(data.columns)}")
            self.log_manager.log_dataframe_in_chunks(data, file_name)

        #self.metadata_manager.update_metadata(f"{file_name}", f'{file_name}_columns', list(data.columns))
        step_2_file_name = f"2-main_file_name:{self.names_of_files_under_procession[0]}, eco_file_name:{self.names_of_files_under_procession[1]}, Fuel:{self.names_of_files_under_procession[2]}"
        self.metadata_manager.update_metadata(step_2_file_name, f'{file_name}_shape', data.shape)

        if self.log_manager:
            self.log_manager.log_info(f"Data Frame Shape: {data.shape}")
        return data

    
    def load_all_data(self) -> List[pd.DataFrame]:
        """
        Load all supported files from the raw data directory.

        Returns:
        - List of DataFrames, one for each successfully loaded file.
        """
        files = self.list_files()
        if not files:
            if self.log_manager:
                self.log_manager.log_error(f"No data files found in directory '{self.raw_data_path}'.")
            raise FileNotFoundError(f"No data files found in directory '{self.raw_data_path}'.")

        data_frames = []
        for file in files:
            try:
                df = self.load_data(file)
                data_frames.append(df)
                if self.log_manager:
                    self.log_manager.log_info(f"Successfully loaded: {file}")
            except Exception as e:
                if self.log_manager:
                    self.log_manager.log_error(f"Failed to load '{file}': {e}")
        
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
            if self.log_manager:
                self.log_manager.log_info(f"Data successfully saved to: {save_path}")
        except Exception as e:
            if self.log_manager:
                self.log_manager.log_error(f"Failed to save data to '{save_path}': {e}")

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
            if self.log_manager:
                self.log_manager.log_warning("Validation failed: DataFrame is empty.")
            return False
        if data.isnull().all(axis=None):
            if self.log_manager:
                self.log_manager.log_warning("Validation failed: DataFrame contains only NaN values.")
            return False
        return True
