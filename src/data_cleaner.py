import pandas as pd
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
        self.names_of_files_under_procession = names_of_files_under_procession
        self.metadata_manager = metadata_manager
        self.log_manager = log_manager
        self.step_5_file_name = None
        if self.log_manager:
            self.log_manager.log_info("DataCleaner initialized.")

    # Removed the _filter_columns method
    # def _filter_columns(self):
    #     # ...method content...

    def handle_missing_values(self) -> pd.DataFrame:
        """
        Handles missing values in the DataFrame.
        Returns:
        - pd.DataFrame: DataFrame with missing values handled.
        """
        #if self.log_manager:
        #    self.log_manager.log_info("Handling missing values.")
        # Example: Fill missing values with mean of each column
        self.df = self.df.fillna(self.df.mean())
        return self.df
    
    def remove_duplicates(self) -> pd.DataFrame:
        """
        Removes duplicate rows from the DataFrame.
        Returns:
        - pd.DataFrame: DataFrame with duplicates removed.
        """
        #if self.log_manager:
        #    self.log_manager.log_info("Removing duplicate rows.")
        self.df = self.df.drop_duplicates()
        return self.df
    
    def handle_outliers(self) -> pd.DataFrame:
        """
        Handles outliers in the DataFrame.
        Returns:
        - pd.DataFrame: DataFrame with outliers handled.
        """
        #if self.log_manager:
        #    self.log_manager.log_info("Handling outliers.")
        # Example: Cap values at the 1st and 99th percentiles
        numerical_columns = self.df.select_dtypes(include=['number']).columns
        for col in numerical_columns:
            lower_bound = self.df[col].quantile(0.01)
            upper_bound = self.df[col].quantile(0.99)
            self.df[col] = self.df[col].clip(lower=lower_bound, upper=upper_bound)
        return self.df

    def clean(self) -> pd.DataFrame:
        """
        Cleans the DataFrame by handling missing values, removing duplicates, and handling outliers.
        Returns:
        - pd.DataFrame: Cleaned DataFrame.
        """
        if self.log_manager:
            self.log_manager.log_info("Starting data cleaning process.")
        # !!! check !!!
        self.handle_missing_values()
        self.remove_duplicates()
        #self.handle_outliers()
        
        if self.log_manager:
            self.log_manager.log_info("Data cleaning process completed.")
        
        return self.df
