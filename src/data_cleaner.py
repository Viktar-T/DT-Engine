import pandas as pd
import logging
from typing import List, Dict, Optional
from tabulate import tabulate

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def split_dataframe(df, chunk_size):
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

def log_dataframe_in_chunks(df, chunk_size=6, rows=3):
    """
    Log a DataFrame in chunks to improve readability.

    Parameters:
    - df: The DataFrame to log.
    - file_name: The name of the file the data was loaded from.
    - chunk_size: The number of columns per chunk.
    - rows: The number of rows to display per chunk.
    """
    for chunk in split_dataframe(df, chunk_size):
        logger.info(f"currrent DataFrame :\n{tabulate(chunk.head(rows), headers='keys', tablefmt='fancy_grid')}")

class DataCleaner:
    def __init__(self, df: pd.DataFrame, required_columns: List[str], time_mapping: Optional[Dict[str, str]] = None):
        """
        Initialize the DataCleaner.

        Parameters:
        - df: DataFrame to preprocess.
        - required_columns: List of required columns for processing.
        - time_mapping: Optional; A dictionary mapping required columns to their respective time columns.
        """
        self.df = df.copy()
        self.required_columns = required_columns
        self.time_mapping = time_mapping
        self.time_suffix = '[ms]'  # Default suffix for time columns

    def map_time_columns(self) -> Dict[str, str]:
        """
        Map required columns to their respective time columns.

        Returns:
        - A dictionary where keys are required columns and values are their time columns.
        """
        logger.info("Mapping time columns to required columns...")
        time_columns = [col for col in self.df.columns if self.time_suffix in col]
        column_map = {}

        if self.time_mapping:  # Use provided mapping if available
            for req_col, time_col in self.time_mapping.items():
                if req_col in self.df.columns and time_col in self.df.columns:
                    column_map[req_col] = time_col
                else:
                    logger.warning(f"Provided mapping invalid for '{req_col}' or '{time_col}'. Skipping...")
        else:  # Dynamically infer mapping based on column adjacency
            for req_col in self.required_columns:
                if req_col in self.df.columns:
                    req_index = self.df.columns.get_loc(req_col)
                    if req_index > 0:
                        potential_time_col = self.df.columns[req_index - 1]
                        if potential_time_col in time_columns:
                            column_map[req_col] = potential_time_col
                        else:
                            logger.warning(f"No adjacent time column found for '{req_col}'.")
                else:
                    logger.warning(f"Column '{req_col}' not found in DataFrame. Skipping...")
        logger.info(f"Mapped columns: {column_map}")
        return column_map

    def filter_required_columns(self, column_map: Dict[str, str]) -> pd.DataFrame:
        """
        Filter the DataFrame to include only the required columns and their corresponding time columns.

        Parameters:
        - column_map: A dictionary mapping required columns to their respective time columns.

        Returns:
        - A DataFrame with only the required columns and their time columns.
        """
        logger.info("Filtering required columns...")
        selected_columns = []

        for req_col, time_col in column_map.items():
            if req_col in self.df.columns and time_col in self.df.columns:
                selected_columns.extend([time_col, req_col])

        if not selected_columns:
            logger.error("No valid columns found. Filtered DataFrame will be empty.")
            return pd.DataFrame()

        filtered_df = self.df[selected_columns].copy()
        logger.info(f"Filtered DataFrame shape: {filtered_df.shape}")
        log_dataframe_in_chunks(filtered_df)
        return filtered_df

    def harmonize_time(self, filtered_df: pd.DataFrame) -> pd.DataFrame:
        """
        Harmonize time for the required columns in the filtered DataFrame.

        Parameters:
        - filtered_df: DataFrame containing only the required columns and their corresponding time columns.

        Returns:
        - A DataFrame with harmonized time for the required columns.
        """
        if filtered_df.empty:
            logger.error("Filtered DataFrame is empty. Skipping time harmonization.")
            return pd.DataFrame()

        logger.info("Starting time harmonization...")
        frames = []

        for i in range(0, len(filtered_df.columns), 2):
            time_col = filtered_df.columns[i]
            value_col = filtered_df.columns[i + 1]

            temp_df = filtered_df[[time_col, value_col]].dropna()
            temp_df = temp_df.drop_duplicates(subset=[time_col])
            temp_df.set_index(time_col, inplace=True)
            # temp_df = temp_df.rename(columns={value_col: f"Harmonized_{value_col}"})
            frames.append(temp_df)

        if not frames:
            logger.error("No valid data to harmonize. Ensure columns contain data.")
            return pd.DataFrame()

        harmonized_df = pd.concat(frames, axis=1).sort_index()
        harmonized_df.interpolate(method="linear", inplace=True)

        logger.info(f"Time harmonization completed. Rows: {len(harmonized_df)}, Columns: {len(harmonized_df.columns)}")
        log_dataframe_in_chunks(harmonized_df)
        return harmonized_df

    def clean_data(self) -> pd.DataFrame:
        """
        Full cleaning pipeline:
        - Map required columns to time columns.
        - Filter required columns.
        - Harmonize time for the filtered columns.

        Returns:
        - The cleaned and harmonized DataFrame.
        """
        logger.info("Starting full data cleaning pipeline...")
        column_map = self.map_time_columns()           # ????????????
        filtered_df = self.filter_required_columns(column_map)
        harmonized_df = self.harmonize_time(filtered_df)
        log_dataframe_in_chunks(harmonized_df)
        logger.info("Data cleaning pipeline completed.")
        return harmonized_df


