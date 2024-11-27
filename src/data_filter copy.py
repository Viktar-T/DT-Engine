import pandas as pd
from typing import List
from src.log_manager import LogManager
from src.metadata_manager import MetadataManager

class DataFilter:
    """
    A class to filter DataFrame columns based on required columns.
    """

    def __init__(self, df: pd.DataFrame, 
                 required_columns: List[List[str]], 
                 names_of_files_under_procession: List[str] = None,
                 metadata_manager: MetadataManager = None, 
                 log_manager: LogManager = None):
        """
        Initialize the DataFilter.

        Parameters:
        - df: The DataFrame to filter.
        - required_columns: A list of lists where each sublist contains time and data columns.
        - names_of_files_under_procession: A list of file names under procession.
        - metadata_manager: An instance of MetadataManager to handle metadata.
        - log_manager: An instance of LogManager for logging.
        """
        self.df = df
        self.required_columns = required_columns
        self.names_of_files_under_procession = names_of_files_under_procession
        self.metadata_manager = metadata_manager
        self.log_manager = log_manager

    def filter_columns(self) -> pd.DataFrame:
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
        if self.metadata_manager:
            self.step_5_file_name = f"5-main_file_name:{self.names_of_files_under_procession[0]}, eco_file_name:{self.names_of_files_under_procession[1]}, Fuel:{self.names_of_files_under_procession[2]}"
            #self.metadata_manager.update_metadata(self.step_5_file_name, 'Cleaned DataFrame columns:', filtered_df.columns)
            self.metadata_manager.update_metadata(self.step_5_file_name, 'cleaned_data_shape', self.df.shape)
        return filtered_df

    def synchronize_time(self) -> pd.DataFrame:
        """
        Synchronizes time for all sensors in the DataFrame.

        Returns:
        - pd.DataFrame: A DataFrame with synchronized time for all sensors.
        """
        import numpy as np

        # Create a list to hold individual sensor DataFrames
        sensor_dfs = []

        for pair in self.required_columns:
            time_col, data_col = pair

            # Check if both columns exist in the DataFrame
            if time_col in self.df.columns and data_col in self.df.columns:
                sensor_df = self.df[[time_col, data_col]].copy()
                sensor_df.dropna(inplace=True)

                # Rename the time column to a common name
                sensor_df.rename(columns={time_col: 'Czas [ms]'}, inplace=True)

                # Adjust time for faster sensors
                if time_col in ['Czas [ms].27', 'Czas [ms].29']:
                    # Downsample by taking every 10th sample
                    sensor_df = sensor_df.iloc[::10, :].reset_index(drop=True)

                sensor_dfs.append(sensor_df)

        # Merge all sensor DataFrames on 'Czas [ms]'
        from functools import reduce
        merged_df = reduce(lambda left, right: pd.merge(left, right, on='Czas [ms]', how='outer'), sensor_dfs)

        # Sort by 'Czas [ms]' and interpolate missing values
        merged_df.sort_values('Czas [ms]', inplace=True)
        merged_df.interpolate(method='linear', inplace=True)

        # Reset index
        merged_df.reset_index(drop=True, inplace=True)

        return merged_df
