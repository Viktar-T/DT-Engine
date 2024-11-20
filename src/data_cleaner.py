import pandas as pd
import numpy as np
import logging
from typing import List, Dict, Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class DataCleaner:
    def __init__(self, df: pd.DataFrame, time_column: Optional[str] = None):
        """
        Initialize the DataCleaner.

        Parameters:
        - df: DataFrame to preprocess.
        - time_column: Optional; The column to use as the time index for harmonization.
        """
        self.df = df.copy()
        self.time_column = time_column

    def harmonize_time(self, parameter_columns: List[str]) -> pd.DataFrame:
        """
        Harmonize time columns across multiple parameters.

        Parameters:
        - parameter_columns: List of parameter columns to align with the time column.

        Returns:
        - A DataFrame with harmonized time columns.
        """
        if not self.time_column or self.time_column not in self.df.columns:
            raise ValueError(f"Time column '{self.time_column}' is missing or not specified.")

        frames = []
        logger.info("Starting time harmonization...")
        for col in parameter_columns:
            if col not in self.df.columns:
                logger.warning(f"Parameter column '{col}' is missing. Skipping...")
                continue
            
            temp_df = self.df[[self.time_column, col]].dropna()
            temp_df = temp_df.drop_duplicates(subset=[self.time_column])
            temp_df.set_index(self.time_column, inplace=True)
            frames.append(temp_df)

        if not frames:
            raise ValueError("No valid parameter columns for time harmonization.")

        harmonized_df = pd.concat(frames, axis=1).sort_index()
        harmonized_df.interpolate(method="linear", inplace=True)
        logger.info(f"Time harmonization completed. Rows: {len(harmonized_df)}, Columns: {len(harmonized_df.columns)}")
        return harmonized_df

    def handle_missing_values(self, strategy: str = "mean") -> pd.DataFrame:
        """
        Handle missing values in the DataFrame.

        Parameters:
        - strategy: The strategy for handling missing values.
                   Options: "mean", "median", "mode", "ffill", "bfill", "drop".

        Returns:
        - DataFrame with missing values handled.
        """
        logger.info(f"Handling missing values with strategy: {strategy}")
        if strategy == "mean":
            self.df.fillna(self.df.mean(), inplace=True)
        elif strategy == "median":
            self.df.fillna(self.df.median(), inplace=True)
        elif strategy == "mode":
            self.df.fillna(self.df.mode().iloc[0], inplace=True)
        elif strategy == "ffill":
            self.df.fillna(method="ffill", inplace=True)
        elif strategy == "bfill":
            self.df.fillna(method="bfill", inplace=True)
        elif strategy == "drop":
            self.df.dropna(inplace=True)
        else:
            raise ValueError(f"Invalid strategy '{strategy}' for handling missing values.")
        
        logger.info("Missing value handling completed.")
        return self.df

    def remove_outliers(self, columns: List[str], threshold: float = 3.0) -> pd.DataFrame:
        """
        Remove outliers from specified columns using Z-score thresholding.

        Parameters:
        - columns: List of columns to check for outliers.
        - threshold: Z-score threshold for defining outliers.

        Returns:
        - DataFrame with outliers removed.
        """
        logger.info(f"Removing outliers with Z-score threshold: {threshold}")
        for col in columns:
            if col not in self.df.columns:
                logger.warning(f"Column '{col}' not found in DataFrame. Skipping...")
                continue

            z_scores = np.abs((self.df[col] - self.df[col].mean()) / self.df[col].std())
            self.df = self.df[z_scores < threshold]
        
        logger.info(f"Outlier removal completed. Rows remaining: {len(self.df)}")
        return self.df

    def normalize_columns(self, columns: List[str], method: str = "min-max") -> pd.DataFrame:
        """
        Normalize specified columns using a chosen method.

        Parameters:
        - columns: List of columns to normalize.
        - method: Normalization method. Options: "min-max", "z-score".

        Returns:
        - DataFrame with normalized columns.
        """
        logger.info(f"Normalizing columns using method: {method}")
        for col in columns:
            if col not in self.df.columns:
                logger.warning(f"Column '{col}' not found in DataFrame. Skipping...")
                continue

            if method == "min-max":
                self.df[col] = (self.df[col] - self.df[col].min()) / (self.df[col].max() - self.df[col].min())
            elif method == "z-score":
                self.df[col] = (self.df[col] - self.df[col].mean()) / self.df[col].std()
            else:
                raise ValueError(f"Invalid normalization method '{method}'.")
        
        logger.info("Normalization completed.")
        return self.df

    def custom_transformation(self, func, **kwargs) -> pd.DataFrame:
        """
        Apply a custom transformation function to the DataFrame.

        Parameters:
        - func: A callable function that accepts the DataFrame and returns a transformed DataFrame.
        - kwargs: Additional keyword arguments for the transformation function.

        Returns:
        - Transformed DataFrame.
        """
        logger.info(f"Applying custom transformation: {func.__name__}")
        self.df = func(self.df, **kwargs)
        logger.info("Custom transformation completed.")
        return self.df

