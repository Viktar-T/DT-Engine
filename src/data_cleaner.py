import pandas as pd
import logging
from typing import List
from src.metadata_manager import MetadataManager
from src.log_manager import LogManager

# Set up logging
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# logger = logging.getLogger(__name__)

class DataCleaner:
    """
    A class to clean and preprocess data for future analysis.
    """

    def __init__(self, df: pd.DataFrame, 
                 required_columns: List[List[str]], 
                 names_of_files_under_procession: List[str] = None,
                 metadata_manager: MetadataManager = None, 
                 log_manager: LogManager = None):
        """
        Initialize the DataCleaner.

        Parameters:
        - df: The DataFrame to clean.
        - required_columns: A list of lists where each sublist contains time and data columns.
        - names_of_files_under_procession: A list of file names under procession.
        - metadata_manager: An instance of MetadataManager to handle metadata.
        - log_manager: An instance of LogManager for logging.
        """
        if not isinstance(df, pd.DataFrame):
            raise TypeError("Input data must be a pandas DataFrame.")
        self.df = df.copy()
        self.required_columns = required_columns
        self.names_of_files_under_procession = names_of_files_under_procession
        self.metadata_manager = metadata_manager
        self.log_manager = log_manager
        self.step_5_file_name = None
        if self.log_manager:
            self.log_manager.log_info("DataCleaner initialized with required columns.")

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
        if missing_columns and self.log_manager:
            self.log_manager.log_warning(f"The following required columns are missing from the DataFrame: {missing_columns}")
            columns_to_keep = [col for col in columns_to_keep if col in self.df.columns]

        # Filter the DataFrame
        filtered_df = self.df[columns_to_keep]
        if self.log_manager:
            self.log_manager.log_info(f"Filtered DataFrame to include {len(columns_to_keep)} columns.")
        return filtered_df

    def clean(self) -> pd.DataFrame:
        """
        Cleans the DataFrame by retaining only the required columns.

        Returns:
        - pd.DataFrame: A cleaned DataFrame.
        """
        if self.log_manager:
            self.log_manager.log_info("Starting data cleaning process.")
        filtered_df = self._filter_columns()
        if self.log_manager:
            self.log_manager.log_info("Data cleaning process completed.")
            self.log_manager.log_info(f"Filtered DataFrame shape: {filtered_df.shape}")
        if self.log_manager:
            self.log_manager.log_dataframe_in_chunks(filtered_df)
        if self.metadata_manager:
            self.step_5_file_name = "5-raw file_name"
            #self.metadata_manager.update_metadata(self.step_5_file_name, 'Cleaned DataFrame columns:', filtered_df.columns)
            self.metadata_manager.update_metadata(self.step_5_file_name, 'cleaned_data_shape', filtered_df.shape)
        return filtered_df
