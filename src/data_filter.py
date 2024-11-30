import numpy as np
import pandas as pd
from typing import List, Tuple
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
        # Convert 'Time' from milliseconds to datetime
        final_df['Time'] = pd.to_datetime(final_df['Time'], unit='ms')

        if self.log_manager:
            self.log_manager.log_info("Time columns synchronized successfully.")

        if self.metadata_manager:
            self.metadata_manager.update_metadata(
                self.step_5_file_name, 'synchronized_data_shape after "synchronize_time()": ', final_df.shape
            )

        # Update self.df with synchronized DataFrame
        self.df = final_df
        self.log_manager.log_info(f"Filtered DataFrame shape. after synchronize_time(): {self.df.shape}")
        #self.log_manager.log_dataframe_in_chunks(self.df)
        # No need to return final_df
        return final_df
    
    
    def identify_stable_rotation(self, threshold: int = 20, window: str = '10000ms') -> List[np.ndarray]:
        """
        Identifies stable rotation levels in 'Obroty[obr/min]'.
        A rotation level is considered stable if its value changes by ≤ threshold over the specified window.

        Returns:
        - List of time arrays corresponding to each stable rotation level.
        """
        if self.log_manager:
            self.log_manager.log_info(f"Starting identify_stable_rotation with threshold={threshold} and window='{window}'")
        
        # Check if 'Obroty[obr/min]' column exists
        if 'Obroty[obr/min]' not in self.df.columns:
            if self.log_manager:
                self.log_manager.log_error("Column 'Obroty[obr/min]' not found in DataFrame.")
            return []

        df = self.df.copy()
        if self.log_manager:
            self.log_manager.log_info("DataFrame copied for processing.")

        # Check if 'Time' column exists
        if 'Time' not in df.columns:
            if self.log_manager:
                self.log_manager.log_error("Column 'Time' not found in DataFrame.")
            return []

        # Ensure 'Time' is in datetime format and set as index
        if not pd.api.types.is_datetime64_any_dtype(df['Time']):
            df['Time'] = pd.to_datetime(df['Time'], unit='ms')
            if self.log_manager:
                self.log_manager.log_info("Converted 'Time' column to datetime.")
        else:
            if self.log_manager:
                self.log_manager.log_info("'Time' column is already in datetime format.")

        df.set_index('Time', inplace=True)
        if self.log_manager:
            self.log_manager.log_info("'Time' column set as index.")

        # Compute rolling max and min over the specified window
        if self.log_manager:
            self.log_manager.log_info("Computing rolling max and min of 'Obroty[obr/min]'.")
        rolling_object = df['Obroty[obr/min]'].rolling(window, min_periods=1)
        roll_max = rolling_object.max()
        roll_min = rolling_object.min()
        roll_diff = roll_max - roll_min

        # Identify stable rotation where difference ≤ threshold
        stable_mask = roll_diff <= threshold
        if self.log_manager:
            num_stable_points = stable_mask.sum()
            self.log_manager.log_info(f"Identified {num_stable_points} points where rotation difference ≤ threshold.")

        # Label contiguous stable regions
        df['Stable'] = stable_mask
        df['Group'] = (df['Stable'] != df['Stable'].shift()).cumsum()
        if self.log_manager:
            num_groups = df['Group'].nunique()
            self.log_manager.log_info(f"Labeled stable regions into {num_groups} groups.")

        # Extract time arrays for each stable region
        stable_time_arrays = []
        for group_id, group in df[df['Stable']].groupby('Group'):
            times = group.index.values
            stable_time_arrays.append(times)
            if self.log_manager:
                self.log_manager.log_info(f"Group {group_id}: Found {len(times)} stable time points.")

        # Clean up
        df.drop(columns=['Stable', 'Group'], inplace=True)
        if self.log_manager:
            self.log_manager.log_info("Dropped temporary 'Stable' and 'Group' columns.")

        # Logging
        if self.log_manager:
            self.log_manager.log_info(f"Identified {len(stable_time_arrays)} stable rotation levels.")

        # Calculate average rotation values for each stable rotation period
        rpm_mean_values = []
        for idx, time_array in enumerate(stable_time_arrays):
            extracted_df = self.df[self.df['Time'].isin(time_array)].copy()
            rpm_mean_value = round(extracted_df['Obroty[obr/min]'].mean(), 1)
            rpm_mean_values.append(rpm_mean_value)
            if self.log_manager:
                self.log_manager.log_info(f"Group {idx}: Mean rotation = {rpm_mean_value} RPM.")

        if self.log_manager:
            self.log_manager.log_info(f"Stable rotation levels extracted. Average values: {rpm_mean_values}")

        # Metadata management
        if self.metadata_manager:
            self.metadata_manager.update_metadata(
                self.step_5_file_name,
                'Identified stable rotation levels:',
                len(stable_time_arrays)
            )
            self.metadata_manager.update_metadata(
                self.step_5_file_name,
                'Stable rotation levels extracted. Average values:',
                rpm_mean_values
            )

        return stable_time_arrays
    
    # !!! NOT USED !!!
    def identify_stable_torque_percent(self, threshold: float = 1.5, window: str = '30000ms') -> List[np.ndarray]:
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
        if self.df.index.name != 'Time':
            if self.log_manager:
                self.log_manager.log_error("Index 'Time' not set in DataFrame.")
            return []

        # Proceed with the rest of the method using df.index for time
        # ...

    def identify_stable_torque_nm(self, threshold: float = 5, window: str = '10000ms') -> List[np.ndarray]:
        """
        Identifies stable torque levels in 'Moment obrotowy[Nm]'.
        A torque level is considered stable if its value changes by ≤ threshold over the specified window.

        Returns:
        - List of time arrays corresponding to each stable torque level.
        """
        if self.log_manager:
            self.log_manager.log_info(f"!!!---> Starting identify_stable_torque_nm with threshold={threshold} and window='{window}'")
        
        # Check if 'Moment obrotowy[Nm]' column exists
        if 'Moment obrotowy[Nm]' not in self.df.columns:
            if self.log_manager:
                self.log_manager.log_error("Column 'Moment obrotowy[Nm]' not found in DataFrame.")
            return []

        df = self.df.copy()
        if self.log_manager:
            self.log_manager.log_info("DataFrame copied for processing.")
        
        # Check if 'Time' column exists
        if 'Time' not in df.columns:
            if self.log_manager:
                self.log_manager.log_error("Column 'Time' not found in DataFrame.")
            return []

        # Ensure 'Time' is in datetime format and set as index
        if not pd.api.types.is_datetime64_any_dtype(df['Time']):
            df['Time'] = pd.to_datetime(df['Time'], unit='ms')
            if self.log_manager:
                self.log_manager.log_info("Converted 'Time' column to datetime.")
        else:
            if self.log_manager:
                self.log_manager.log_info("'Time' column is already in datetime format.")
        
        df.set_index('Time', inplace=True)
        if self.log_manager:
            self.log_manager.log_info("'Time' column set as index.")

        # Compute rolling max and min over the specified window
        if self.log_manager:
            self.log_manager.log_info("Computing rolling max and min of 'Moment obrotowy[Nm]'.")
        rolling_object = df['Moment obrotowy[Nm]'].rolling(window, min_periods=1)
        roll_max = rolling_object.max()
        roll_min = rolling_object.min()
        roll_diff = roll_max - roll_min
        if self.log_manager:
            #  Log main parameters of the rolling object
            self.log_manager.log_info("Rolling object parameters:")
            self.log_manager.log_info(f" - window: {rolling_object.window}")
            self.log_manager.log_info(f" - min_periods: {rolling_object.min_periods}")
            self.log_manager.log_info(f" - center: {rolling_object.center}")
            self.log_manager.log_info(f" - win_type: {rolling_object.win_type}")
            self.log_manager.log_info(f" - axis: {rolling_object.axis}")
            self.log_manager.log_info(f" - method: {rolling_object.method}")
            self.log_manager.log_info(f" - closed: {rolling_object.closed}")

        # Identify stable torque where difference ≤ threshold
        stable_mask = roll_diff <= threshold
        if self.log_manager:
            num_stable_points = stable_mask.sum()
            self.log_manager.log_info(f"Identified {num_stable_points} points where torque difference ≤ threshold.")

        # Label contiguous stable regions
        df['Stable'] = stable_mask
        df['Group'] = (df['Stable'] != df['Stable'].shift()).cumsum()
        if self.log_manager:
            num_groups = df['Group'].nunique()
            self.log_manager.log_info(f"Labeled stable regions into {num_groups} groups.")

        # Extract time arrays for each stable region
        stable_time_arrays = []
        for group_id, group in df[df['Stable']].groupby('Group'):
            times = group.index.values
            stable_time_arrays.append(times)
            if self.log_manager:
                self.log_manager.log_info(f"Group {group_id}: Found {len(times)} stable time points.")

        # Clean up
        df.drop(columns=['Stable', 'Group'], inplace=True)
        if self.log_manager:
            self.log_manager.log_info("Dropped temporary 'Stable' and 'Group' columns.")

        # Logging
        if self.log_manager:
            self.log_manager.log_info(f"Identified {len(stable_time_arrays)} stable torque levels.")

        # Calculate average torque values for each stable torque period
        torque_mean_values = []
        for idx, time_array in enumerate(stable_time_arrays):
            extracted_df = self.df[self.df['Time'].isin(time_array)].copy()
            torque_mean_value = round(extracted_df['Moment obrotowy[Nm]'].mean(), 1)
            torque_mean_values.append(torque_mean_value)
            if self.log_manager:
                self.log_manager.log_info(f"Group {idx}: Mean torque = {torque_mean_value} Nm.")

        if self.log_manager:
            self.log_manager.log_info(f"Stable torque levels extracted. Average values: {torque_mean_values}")

        # Metadata management
        if self.metadata_manager:
            self.metadata_manager.update_metadata(
                self.step_5_file_name,
                'Identified stable torque levels (Moment obrotowy[Nm]):',
                len(stable_time_arrays)
            )
            self.metadata_manager.update_metadata(
                self.step_5_file_name,
                'Stable torque levels extracted. Average values:',
                torque_mean_values
            )

        return stable_time_arrays

    # !!! NOT USED !!! -> add data clenning to each chunck (window)
    def identify_stable_fuel_consumption(self, threshold: float = 0.5, window: str = '10000ms') -> List[np.ndarray]:
        """
        Identify stable fuel consumption levels in the column 'Zużycie paliwa średnie[g/s]'.
        A fuel consumption level is considered stable if its value changes by ≤ threshold over a specified window.
        Returns a list of time arrays (from the "Time" index) corresponding to each stable fuel consumption level.
        """

        df = self.df.copy()

        # Ensure 'Time' is in datetime format and set as index
        if not pd.api.types.is_datetime64_any_dtype(df['Time']):
            df['Time'] = pd.to_datetime(df['Time'], unit='ms')
        df.set_index('Time', inplace=True)

        # Compute rolling max and min over the specified window
        roll_max = df['Zużycie paliwa średnie[g/s]'].rolling(window, min_periods=1).max()
        roll_min = df['Zużycie paliwa średnie[g/s]'].rolling(window, min_periods=1).min()
        roll_diff = roll_max - roll_min

        # Identify stable fuel consumption where difference ≤ threshold
        stable_mask = roll_diff <= threshold

        # Label contiguous stable regions
        df['Stable'] = stable_mask
        df['Group'] = (df['Stable'] != df['Stable'].shift()).cumsum()

        # Extract time arrays for each stable region
        stable_time_arrays = []
        for _, group in df[df['Stable']].groupby('Group'):
            times = group.index.values
            stable_time_arrays.append(times)

        # Clean up
        df.drop(columns=['Stable', 'Group'], inplace=True)

        # Logging
        if self.log_manager:
            self.log_manager.log_info(f"Identified {len(stable_time_arrays)} stable fuel consumption levels.")

        fuel_mean_values = []
        for time_array in stable_time_arrays:
            time_array_sorted = sorted(time_array)
            extracted_df = self.df[self.df['Time'].isin(time_array_sorted)].copy()
            fuel_mean_value = round(extracted_df['Zużycie paliwa średnie[g/s]'].mean(), 2)
            fuel_mean_values.append(fuel_mean_value)

        if self.log_manager:
            self.log_manager.log_info(f"Stable fuel consumption levels extracted. Average values: {fuel_mean_values}")
        
        # Metadata management
        if self.metadata_manager:
            self.metadata_manager.update_metadata(
                self.step_5_file_name, 'Identified stable fuel consumption levels:', len(stable_time_arrays)
            )
            self.metadata_manager.update_metadata(
                self.step_5_file_name, 'Stable fuel consumption levels extracted. Average values:', fuel_mean_values
            )

        return stable_time_arrays

    def filter_stable_periods(self) -> pd.DataFrame:
        """
        Intersects the time arrays from multiple stable identification functions,
        extracts corresponding data from the DataFrame.

        Returns:
        - pd.DataFrame with the extracted data.
        """
        stable_functions = [
            self.identify_stable_rotation,
            self.identify_stable_torque_nm,
            #self.identify_stable_torque_percent,
            #self.identify_stable_fuel_consumption,
            # Add more functions if needed
        ]

        # Collect all stable time sets
        stable_time_sets = []
        for func in stable_functions:
            stable_time_arrays = func()
            if not stable_time_arrays:
                if self.log_manager:
                    self.log_manager.log_warning(f"No stable periods identified by {func.__name__}.")
                return pd.DataFrame()
            # Combine all time arrays from this function into a set
            times = set(np.concatenate(stable_time_arrays))
            stable_time_sets.append(times)

        # Find the intersection of all stable times
        intersected_times = set.intersection(*stable_time_sets)

        if not intersected_times:
            if self.log_manager:
                self.log_manager.log_warning("No overlapping stable periods found among all metrics.")
            return pd.DataFrame()

        extracted_df = self.df[self.df['Time'].isin(intersected_times)].copy()

        if self.log_manager:
            self.log_manager.log_info(f"Extracted {len(extracted_df)} rows of intersected stable data.")

        if self.metadata_manager:
            self.metadata_manager.update_metadata(
                self.step_5_file_name, 'filtered_data_shape after "extract_and_clean_data()":', extracted_df.shape
            )

        return extracted_df
