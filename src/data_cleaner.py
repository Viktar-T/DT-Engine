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

    def _handle_missing_values(self) -> pd.DataFrame:
        """
        Handles missing values in the DataFrame.
        Returns:
        - pd.DataFrame: DataFrame with missing values handled.
        """
        if self.log_manager:
            self.log_manager.log_info("Handling missing values.")
        # Example: Fill missing values with mean of each column
        self.df = self.df.fillna(self.df.mean())
        return self.df
    
    def _remove_duplicates(self) -> pd.DataFrame:
        """
        Removes duplicate rows from the DataFrame.
        Returns:
        - pd.DataFrame: DataFrame with duplicates removed.
        """
        if self.log_manager:
            self.log_manager.log_info("Removing duplicate rows.")
        self.df = self.df.drop_duplicates()
        return self.df
    
    def _handle_outliers(self) -> pd.DataFrame:
        """
        Handles outliers in the DataFrame.
        Returns:
        - pd.DataFrame: DataFrame with outliers handled.
        """
        if self.log_manager:
            self.log_manager.log_info("Handling outliers.")
        # Example: Cap values at the 1st and 99th percentiles
        numerical_columns = self.df.select_dtypes(include=['number']).columns
        for col in numerical_columns:
            lower_bound = self.df[col].quantile(0.01)
            upper_bound = self.df[col].quantile(0.99)
            self.df[col] = self.df[col].clip(lower=lower_bound, upper=upper_bound)
        return self.df

    def clean(self) -> pd.DataFrame:
        """
        Cleans the DataFrame by retaining only the required columns,
        handling missing values, removing duplicates, and handling outliers.

        Returns:
        - pd.DataFrame: A cleaned DataFrame.
        """
        if self.log_manager:
            self.log_manager.log_info("Starting data cleaning process.")
        filtered_df = self._filter_columns()
        self.df = filtered_df  # Update self.df with filtered columns
        #self._handle_missing_values() # the number of "Non-Null Count" in DataFrame after cleaning 3853*10=38530 -> see logs
        self._remove_duplicates()
        self._handle_outliers()
        if self.log_manager:
            self.log_manager.log_info("Data cleaning process completed.")
            self.log_manager.log_info(f"Filtered DataFrame shape: {filtered_df.shape}")
        if self.log_manager:
            self.log_manager.log_dataframe_in_chunks(filtered_df)
        if self.metadata_manager:
            self.step_5_file_name = f"5-main_file_name:{self.names_of_files_under_procession[0]}, eco_file_name:{self.names_of_files_under_procession[1]}, Fuel:{self.names_of_files_under_procession[2]}"
            #self.metadata_manager.update_metadata(self.step_5_file_name, 'Cleaned DataFrame columns:', filtered_df.columns)
            self.metadata_manager.update_metadata(self.step_5_file_name, 'cleaned_data_shape', filtered_df.shape)
        return self.df
