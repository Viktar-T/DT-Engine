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
    
    def synchronize_time(self) -> pd.DataFrame:
        """
        Synchronizes time for all sensors in the DataFrame.
        dose not work consider fast sensors

        Returns:
        - pd.DataFrame: A DataFrame with synchronized time columns.
        """
        sensors = [pair for pair in self.required_columns]
        # Use the time column of the first sensor as the reference time
        reference_time_col = sensors[0][0]

        if reference_time_col not in self.df.columns:
            if self.log_manager:
                self.log_manager.log_error(f"Reference time column '{reference_time_col}' is missing from the DataFrame.")
            return self.df

        # Create a reference time index
        reference_time = self.df[reference_time_col].dropna().astype(float).sort_values().unique()
        final_df = pd.DataFrame(index=reference_time)
        final_df.index.name = 'Time'

        # Calculate average sampling interval for tolerance
        delta_times = np.diff(reference_time)
        average_delta_time = np.mean(delta_times)
        tolerance = average_delta_time / 2

        # Process slow sensors
        for time_col, data_col in sensors:
            if time_col in self.df.columns and data_col in self.df.columns:
                df_sensor = self.df[[time_col, data_col]].dropna()
                df_sensor[time_col] = df_sensor[time_col].astype(float)
                df_sensor.set_index(time_col, inplace=True)
                df_sensor = df_sensor[~df_sensor.index.duplicated(keep='first')]
                # Reindex onto reference time using 'nearest' method
                df_sensor = df_sensor.reindex(final_df.index, method='nearest', tolerance=tolerance)
                final_df[data_col] = df_sensor[data_col]
            else:
                if self.log_manager:
                    self.log_manager.log_warning(f"Columns '{time_col}' and/or '{data_col}' not found in DataFrame.")

        # Reset index to make 'Time' a column
        final_df.reset_index(inplace=True)

        if self.log_manager:
            self.log_manager.log_info("Time columns synchronized successfully.")

        if self.metadata_manager:
            self.metadata_manager.update_metadata(
                self.step_5_file_name, 'synchronized_data_shape', final_df.shape
            )

        return final_df
    
    

  
    # Note USED: slow sensors and fast sensors (interpolation) -- doesnot work well
    def synchronize_time_NOT_USE(self) -> pd.DataFrame:
        """
        Synchronizes time for all sensors in the DataFrame.

        Returns:
        - pd.DataFrame: A DataFrame with synchronized time columns.
        """

        # Identify fast and slow sensors
        fast_sensors = [
            ['Czas [ms].27', 'Moment obrotowy[Nm]'],
            ['Czas [ms].29', 'Obroty[obr/min]']
        ]
        slow_sensors = [pair for pair in self.required_columns if pair not in fast_sensors]
        # slow_sensors = [pair for pair in self.required_columns]
        # Use the time column of the first slow sensor as the reference time
        reference_time_col = slow_sensors[0][0]

        if reference_time_col not in self.df.columns:
            if self.log_manager:
                self.log_manager.log_error(f"Reference time column '{reference_time_col}' is missing from the DataFrame.")
            return self.df

        # Create a reference time index
        reference_time = self.df[reference_time_col].dropna().astype(float).sort_values().unique()
        final_df = pd.DataFrame(index=reference_time)
        final_df.index.name = 'Time'

        # Calculate average sampling interval for tolerance
        delta_times = np.diff(reference_time)
        average_delta_time = np.mean(delta_times)
        tolerance = average_delta_time / 2

        # Process slow sensors
        for time_col, data_col in slow_sensors:
            if time_col in self.df.columns and data_col in self.df.columns:
                df_sensor = self.df[[time_col, data_col]].dropna()
                df_sensor[time_col] = df_sensor[time_col].astype(float)
                df_sensor.set_index(time_col, inplace=True)
                df_sensor = df_sensor[~df_sensor.index.duplicated(keep='first')]
                # Reindex onto reference time using 'nearest' method
                df_sensor = df_sensor.reindex(final_df.index, method='nearest', tolerance=tolerance)
                final_df[data_col] = df_sensor[data_col]
            else:
                if self.log_manager:
                    self.log_manager.log_warning(f"Columns '{time_col}' and/or '{data_col}' not found in DataFrame.")

        # Process fast sensors
        for time_col, data_col in fast_sensors:
            if time_col in self.df.columns and data_col in self.df.columns:
                df_sensor = self.df[[time_col, data_col]].dropna()
                df_sensor[time_col] = df_sensor[time_col].astype(float)
                df_sensor.set_index(time_col, inplace=True)
                df_sensor = df_sensor[~df_sensor.index.duplicated(keep='first')]
                # Interpolate data onto reference time
                df_sensor = df_sensor.reindex(final_df.index).interpolate(method='index')
                final_df[data_col] = df_sensor[data_col]
            else:
                if self.log_manager:
                    self.log_manager.log_warning(f"Columns '{time_col}' and/or '{data_col}' not found in DataFrame.")

        # Reset index to make 'Time' a column
        final_df.reset_index(inplace=True)

        if self.log_manager:
            self.log_manager.log_info("Time columns synchronized successfully.")

        if self.metadata_manager:
            self.metadata_manager.update_metadata(
                self.step_5_file_name, 'synchronized_data_shape', final_df.shape
            )

        return final_df

    def stable_rotation_identification(self) -> List[np.ndarray]:
        """
        Identifies stable rotation levels in 'Obroty[obr/min]'.

        Returns:
        - List of time arrays corresponding to each stable rotation level.
        """
        if 'Obroty[obr/min]' not in self.df.columns:
            if self.log_manager:
                self.log_manager.log_error("Column 'Obroty[obr/min]' not found in DataFrame.")
            return []

        rolling_std = self.df['Obroty[obr/min]'].rolling(window=10000, min_periods=1).std()
        stable_mask = rolling_std <= 0.1
        stable_times = self.df.loc[stable_mask, 'Time']

        # Group consecutive times into periods
        stable_periods = []
        if not stable_times.empty:
            time_diff = stable_times.diff().fillna(0)
            gaps = time_diff > (stable_times.diff().median() * 1.5)
            gap_indices = stable_times[gaps].index.tolist()
            start_idx = 0
            for gap_idx in gap_indices:
                period = stable_times.iloc[start_idx:gap_idx]
                stable_periods.append(period.values)
                start_idx = gap_idx
            # Add the last period
            period = stable_times.iloc[start_idx:]
            stable_periods.append(period.values)

        if self.log_manager:
            self.log_manager.log_info(f"Identified {len(stable_periods)} stable rotation periods.")
        return stable_periods

    def stable_torque_identification(self) -> List[np.ndarray]:
        """
        Identifies stable torque levels in 'Moment obrotowy[Nm]'.

        Returns:
        - List of time arrays corresponding to each stable torque level.
        """
        if 'Moment obrotowy[Nm]' not in self.df.columns:
            if self.log_manager:
                self.log_manager.log_error("Column 'Moment obrotowy[Nm]' not found in DataFrame.")
            return []

        rolling_std = self.df['Moment obrotowy[Nm]'].rolling(window=10000, min_periods=1).std()
        stable_mask = rolling_std <= 0.1
        stable_times = self.df.loc[stable_mask, 'Time']

        # Group consecutive times into periods
        stable_periods = []
        if not stable_times.empty:
            time_diff = stable_times.diff().fillna(0)
            gaps = time_diff > (stable_times.diff().median() * 1.5)
            gap_indices = stable_times[gaps].index.tolist()
            start_idx = 0
            for gap_idx in gap_indices:
                period = stable_times.iloc[start_idx:gap_idx]
                stable_periods.append(period.values)
                start_idx = gap_idx
            # Add the last period
            period = stable_times.iloc[start_idx:]
            stable_periods.append(period.values)

        if self.log_manager:
            self.log_manager.log_info(f"Identified {len(stable_periods)} stable torque periods.")
        return stable_periods

    def extract_and_clean_data(self) -> pd.DataFrame:
        """
        Extracts data where both rotation and torque are stable and cleans it.

        Returns:
        - pd.DataFrame: Cleaned DataFrame with stable rotation and torque.
        """
        # Get stable periods
        rotation_periods = self.stable_rotation_identification()
        torque_periods = self.stable_torque_identification()

        # Intersect periods
        stable_times = set()
        for r_period in rotation_periods:
            for t_period in torque_periods:
                common_times = set(r_period).intersection(t_period)
                stable_times.update(common_times)

        stable_times = sorted(stable_times)
        if not stable_times:
            if self.log_manager:
                self.log_manager.log_warning("No overlapping stable periods found.")
            return self.df

        # Extract stable data
        stable_df = self.df[self.df['Time'].isin(stable_times)].copy()

        # Clean data using DataCleaner
        data_cleaner = DataCleaner(
            df=stable_df,
            names_of_files_under_procession=self.names_of_files_under_procession,
            metadata_manager=self.metadata_manager,
            log_manager=self.log_manager
        )
        # Call cleaning methods
        data_cleaner.handle_outliers()
        data_cleaner.remove_duplicates()
        data_cleaner.handle_missing_values()

        if self.log_manager:
            self.log_manager.log_info("Data extracted and cleaned successfully.")

        return data_cleaner.df
