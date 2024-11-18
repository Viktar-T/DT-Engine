import os
import pandas as pd
from typing import List, Union


# !!! NOT finished. In the process of development.

class DataLoader:
    """
    A class to handle loading raw data from various formats.
    Supports CSV and Excel files and includes basic validation checks.
    """

    def __init__(self, raw_data_path: str):
        """
        Initialize the DataLoader.

        Parameters:
        - raw_data_path: Path to the directory containing raw data files.
        """
        self.raw_data_path = raw_data_path
        if not os.path.exists(self.raw_data_path):
            raise FileNotFoundError(f"Directory '{self.raw_data_path}' does not exist.")
    
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
               any(f.lower().endswith(ext) for ext in extensions)
        ]
        return files
    
    def load_file(self, file_path: str) -> pd.DataFrame:
        """
        Load a single file into a pandas DataFrame.

        Parameters:
        - file_path: Path to the file to load.

        Returns:
        - DataFrame containing the file's data.
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File '{file_path}' not found.")

        file_extension = os.path.splitext(file_path)[1].lower()

        if file_extension == ".csv":
            data = pd.read_csv(file_path)
        elif file_extension in [".xls", ".xlsx"]:
            data = pd.read_excel(file_path)
        else:
            raise ValueError(f"Unsupported file format: {file_extension}")
        
        # Validate that the DataFrame is not empty
        if data.empty:
            raise ValueError(f"File '{file_path}' is empty or contains no data.")
        
        return data
    
    def load_all_data(self) -> List[pd.DataFrame]:
        """
        Load all supported files from the raw data directory.

        Returns:
        - List of DataFrames, one for each successfully loaded file.
        """
        files = self.list_files()
        if not files:
            raise FileNotFoundError(f"No data files found in directory '{self.raw_data_path}'.")

        data_frames = []
        for file in files:
            try:
                df = self.load_file(file)
                data_frames.append(df)
                print(f"Successfully loaded: {file}")
            except Exception as e:
                print(f"Failed to load '{file}': {e}")
        
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
            print(f"Data successfully saved to: {save_path}")
        except Exception as e:
            print(f"Failed to save data to '{save_path}': {e}")
