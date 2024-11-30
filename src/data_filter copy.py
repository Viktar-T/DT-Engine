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

    def filter_columns(self) -> None:
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
            self.metadata_manager.update_metadata(self.step_5_file_name, 'filtered_data_shape after "def filter_columns":', self.df.shape)
        
        # Update self.df with filtered DataFrame
        self.df = filtered_df
        # No need to return filtered_df

    def synchronize_time(self) -> None:
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
                self.step_5_file_name, 'synchronized_data_shape after "synchronize_time()": ', final_df.shape
            )

        # Update self.df with synchronized DataFrame
        self.df = final_df
        self.log_manager.log_info(f"!!!! ---> Filtered DataFrame shape: {self.df.shape}")
        #self.log_manager.log_dataframe_in_chunks(self.df)
        # No need to return final_df
    
    
    def identify_stable_rotation(self, threshold: int = 20, window: str = '10000ms') -> List[np.ndarray]:
        """
        Identifies stable rotation levels in 'Obroty[obr/min]'.
        A rotation level is considered stable if its value changes by ≤ 20 RPM over a 10-second window.

        Returns:
        - List of time arrays corresponding to each stable rotation level.
        """
        if 'Obroty[obr/min]' not in self.df.columns:
            if self.log_manager:
                self.log_manager.log_error("Column 'Obroty[obr/min]' not found in DataFrame.")
            return []

        df = self.df.copy()
        if 'Time' not in df.columns:
            if self.log_manager:
                self.log_manager.log_error("Column 'Time' not found in DataFrame.")
            return []

        df['Timedelta'] = pd.to_timedelta(df['Time'], unit='ms')
        df.set_index('Timedelta', inplace=True)

        # Use rolling window of 10 seconds ('10s')
        rolling_window = df['Obroty[obr/min]'].rolling(window)
        rotation_range = rolling_window.max() - rolling_window.min()
        # max fluctuation in 10 seconds is threshold=20 RPM
        stable_mask = rotation_range.le(threshold)
        stable_periods = (stable_mask != stable_mask.shift()).cumsum()

        stable_time_arrays = []
        for _, group in df[stable_mask].groupby(stable_periods):
            time_array = group['Time'].values
            stable_time_arrays.append(time_array)

        if self.log_manager:
            self.log_manager.log_info(f"Identified {len(stable_time_arrays)} stable rotation levels.")

        rpm_mean_values = []
        for time_array in stable_time_arrays:
            time_array_sorted = sorted(time_array)
            extracted_df = self.df[self.df['Time'].isin(time_array_sorted)].copy()
            rpm_mean_value = round(extracted_df["Obroty[obr/min]"].mean(), 1)
            rpm_mean_values.append(rpm_mean_value)

        if self.log_manager:
            self.log_manager.log_info(f"Stable rotation levels extracted. Average values: {rpm_mean_values}")
        
        if self.metadata_manager:
            self.metadata_manager.update_metadata(
                self.step_5_file_name, 'Identified stable rotation levels.": ', len(stable_time_arrays)
            )
            self.metadata_manager.update_metadata(
                self.step_5_file_name, 'Stable rotation levels extracted. Average values:', rpm_mean_values
            )

        return stable_time_arrays

    def identify_stable_torque(self, threshold: float = 0.1, window: str = '10000ms') -> List[np.ndarray]:
        """
        Identifies stable torque levels in 'Moment obrotowy[Nm]'.
        A torque value is considered stable if the change between consecutive readings is ≤ 10% of the average value over a 10-second window.

        Returns:
        - List of time arrays corresponding to each stable torque level.
        """
        if 'Moment obrotowy[Nm]' not in self.df.columns:
            if self.log_manager:
                self.log_manager.log_error("Column 'Moment obrotowy[Nm]' not found in DataFrame.")
            return []

        df = self.df.copy()
        if 'Time' not in df.columns:
            if self.log_manager:
                self.log_manager.log_error("Column 'Time' not found in DataFrame.")
            return []

        df['Timedelta'] = pd.to_timedelta(df['Time'], unit='ms')
        df.set_index('Timedelta', inplace=True)

        # Calculate absolute change between consecutive readings
        df['Torque_diff'] = df['Moment obrotowy[Nm]'].diff().abs()

        # Calculate rolling average torque over 10-second window
        rolling_avg = df['Moment obrotowy[Nm]'].rolling(window).mean()

        # Calculate maximum torque difference over 10-second window
        rolling_max_diff = df['Torque_diff'].rolling(window).max()

        # Identify where the maximum torque difference is less than or equal to 10% of rolling average
        df['Torque_stable'] = rolling_max_diff <= threshold * rolling_avg

        stable_mask = df['Torque_stable']
        stable_periods = (stable_mask != stable_mask.shift()).cumsum()

        stable_time_arrays = []
        for _, group in df[stable_mask].groupby(stable_periods):
            time_array = group['Time'].values
            stable_time_arrays.append(time_array)

        if self.log_manager:
            self.log_manager.log_info(f"Identified {len(stable_time_arrays)}")

        # Calculate average torque values for each stable torque period
        torque_mean_values = []
        for time_array in stable_time_arrays:
            time_array_sorted = sorted(time_array)
            extracted_df = self.df[self.df['Time'].isin(time_array_sorted)].copy()
            torque_mean_value = round(extracted_df["Moment obrotowy[Nm]"].mean(), 1)
            torque_mean_values.append(torque_mean_value)

        if self.log_manager:
            self.log_manager.log_info(f"Stable torque ('Moment obrotowy[Nm]') levels extracted. Average values: {torque_mean_values}")

        if self.metadata_manager:
            self.metadata_manager.update_metadata(
                self.step_5_file_name, 'Identified stable torque (Moment obrotowy[Nm]) levels.: ', len(stable_time_arrays)
            )
            self.metadata_manager.update_metadata(
                self.step_5_file_name, 'Stable torque levels extracted. Average values:', torque_mean_values
            )

        # Clean up temporary columns
        df.drop(columns=['Torque_diff', 'Torque_stable'], inplace=True)

        return stable_time_arrays

    def extract_and_clean_data(self) -> pd.DataFrame:
        """
        Intersects the time arrays from stable rotation and torque,
        extracts corresponding data from the DataFrame, and cleans it.

        Returns:
        - pd.DataFrame with the cleaned data.
        """
        stable_rotations = self.identify_stable_rotation()
        stable_torques = self.identify_stable_torque()

        # Find the intersection of time arrays
        intersected_times = set()
        for rot_times in stable_rotations:
            for torque_times in stable_torques:
                common_times = np.intersect1d(rot_times, torque_times)
                intersected_times.update(common_times)

        if not intersected_times:
            if self.log_manager:
                self.log_manager.log_warning("No overlapping stable periods found between rotation and torque.")
            return pd.DataFrame()

        intersected_times = sorted(intersected_times)
        extracted_df = self.df[self.df['Time'].isin(intersected_times)].copy()

        if self.log_manager:
            self.log_manager.log_info(f"Extracted {len(extracted_df)} rows of intersected stable data.")

        if self.metadata_manager:
            self.metadata_manager.update_metadata(
                self.step_5_file_name, 'filtered_data_shape after "extract_and_clean_data()": ', extracted_df.shape
            )

        # # Initialize DataCleaner and perform cleaning operations
        # data_cleaner = DataCleaner(
        #     df=extracted_df,
        #     names_of_files_under_procession=self.names_of_files_under_procession,
        #     metadata_manager=self.metadata_manager,
        #     log_manager=self.log_manager
        # )

        # # Placeholder calls to cleaning functions
        # data_cleaner.handle_outliers()
        # data_cleaner.remove_duplicates()
        # data_cleaner.handle_missing_values()

        # cleaned_df = data_cleaner.df

        # Return the cleaned DataFrame
        return extracted_df
