import pandas as pd
import logging
from typing import List
from tabulate import tabulate
from src.metadata_manager import MetadataManager

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def split_dataframe(df: pd.DataFrame, chunk_size: int):
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

def log_dataframe_in_chunks(df: pd.DataFrame, chunk_size: int = 6, rows: int = 3):
    """
    Log a DataFrame in chunks to improve readability.

    Parameters:
    - df: The DataFrame to log.
    - chunk_size: The number of columns per chunk.
    - rows: The number of rows to display per chunk.
    """
    for chunk in split_dataframe(df, chunk_size):
        logger.info(f"Current DataFrame chunk:\n{tabulate(chunk.head(rows), headers='keys', tablefmt='fancy_grid')}")

class DataCleaner:
    """
    A class to clean and preprocess data for future analysis.
    """

    def __init__(self, df: pd.DataFrame, required_columns: List[List[str]], metadata_manager: MetadataManager = None):
        """
        Initialize the DataCleaner with the list of required columns.

        Parameters:
        - df (pd.DataFrame): The DataFrame to clean.
        - required_columns (List[List[str]]): A list of lists where each sublist contains
                                              a time column and its corresponding data column.
        - metadata_manager (MetadataManager, optional): An instance of MetadataManager to handle metadata.
        """
        if not isinstance(df, pd.DataFrame):
            raise TypeError("Input data must be a pandas DataFrame.")
        
        self.df = df.copy()
        self.required_columns = required_columns
        self.metadata_manager = metadata_manager
        logger.info("DataCleaner initialized with required columns.")

    def _filter_columns(self) -> pd.DataFrame:
        """
        Filters the DataFrame to include only the required columns.

        Returns:
        - pd.DataFrame: A filtered DataFrame containing only the required columns.
        """
        # Flatten the list of required columns
        columns_to_keep = [col for pair in self.required_columns for col in pair]

        # Validate columns exist in the DataFrame
        missing_columns = [col for col in columns_to_keep if col not in self.df.columns]
        if missing_columns:
            logger.warning(f"The following required columns are missing from the DataFrame: {missing_columns}")
            columns_to_keep = [col for col in columns_to_keep if col in self.df.columns]

        # Filter the DataFrame
        filtered_df = self.df[columns_to_keep]
        logger.info(f"Filtered DataFrame to include {len(columns_to_keep)} columns.")
        return filtered_df

    def clean(self) -> pd.DataFrame:
        """
        Cleans the DataFrame by retaining only the required columns.

        Returns:
        - pd.DataFrame: A cleaned DataFrame.
        """
        logger.info("Starting data cleaning process.")
        filtered_df = self._filter_columns()
        logger.info("Data cleaning process completed.")
        logger.info(f"Filtered DataFrame shape: {filtered_df.shape}")
        log_dataframe_in_chunks(filtered_df)
        if self.metadata_manager:
            self.metadata_manager.update_metadata('cleaned_data_shape', filtered_df.shape)
        return filtered_df

