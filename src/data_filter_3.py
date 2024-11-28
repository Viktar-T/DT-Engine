import numpy as np
import pandas as pd
from typing import List
from src.log_manager import LogManager
from src.metadata_manager import MetadataManager
from src.data_cleaner import DataCleaner

class DataFilter:
    """
    A class to filter DataFrame columns based on required columns.
    """

    def __init__(self, df: pd.DataFrame, 
                 required_columns: List[List[str]], 
                 names_of_files_under_procession: List[str] = None,
                 metadata_manager: MetadataManager = None, 
                 log_manager: LogManager = None, 
                 data_cleaner: DataCleaner = None):
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
    
    def synchronize_time(self, reference_time_col: str) -> pd.DataFrame:
        """
        Synchronizes time across all sensors in the DataFrame based on a reference time column.

        Parameters:
        - reference_time_col (str): The reference time column to align all data to.

        Returns:
        - pd.DataFrame: A DataFrame with synchronized time for all sensors.
        """
        # Ensure the reference time column exists
        if reference_time_col not in self.df.columns:
            raise ValueError(f"Reference time column '{reference_time_col}' not found in DataFrame.")

        # Drop rows where reference time is null
        self.df = self.df.dropna(subset=[reference_time_col])

        # Convert reference time column to datetime
        self.df[reference_time_col] = pd.to_datetime(
            self.df[reference_time_col], 
            unit='ms', 
            errors='coerce'
        )
        self.df = self.df.dropna(subset=[reference_time_col])  # Drop rows with invalid datetime conversion

        # Create a common time index
        common_time_index = pd.date_range(
            start=self.df[reference_time_col].min(),
            end=self.df[reference_time_col].max(),
            freq='10ms'  # Define the frequency (e.g., 10ms)
        )
        common_time_df = pd.DataFrame({reference_time_col: common_time_index})

        # Interpolate and align sensor data
        synchronized_dfs = []
        for time_col, sensor_col in self.required_columns:
            if time_col in self.df.columns and sensor_col in self.df.columns:
                # Drop rows with null values in sensor-specific time column
                sensor_data = self.df[[time_col, sensor_col]].dropna()

                # Convert sensor-specific time column to datetime
                sensor_data[time_col] = pd.to_datetime(
                    sensor_data[time_col], 
                    unit='ms', 
                    errors='coerce'
                )
                sensor_data = sensor_data.dropna(subset=[time_col])  # Drop invalid datetime rows

                # Merge and interpolate
                aligned_sensor_data = pd.merge_asof(
                    common_time_df,
                    sensor_data.sort_values(time_col),
                    left_on=reference_time_col,
                    right_on=time_col,
                    direction='nearest'
                )
                aligned_sensor_data = aligned_sensor_data.drop(columns=[time_col])  # Drop original time column
                synchronized_dfs.append(aligned_sensor_data)
            else:
                if self.log_manager:
                    self.log_manager.log_warning(f"Missing sensor data for columns: {time_col}, {sensor_col}")

        # Combine synchronized data
        synchronized_df = pd.concat(synchronized_dfs, axis=1)
        synchronized_df[reference_time_col] = common_time_df[reference_time_col]  # Add common time back

        if self.log_manager:
            self.log_manager.log_info(f"Time synchronized for all sensors using reference column '{reference_time_col}'.")
        if self.metadata_manager:
            self.metadata_manager.update_metadata(self.step_5_file_name, "synchronized_data_shape", synchronized_df.shape)

        return synchronized_df
    
        