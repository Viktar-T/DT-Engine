import json
import numpy as np
import pandas as pd
from typing import List, Optional
from src.log_manager import LogManager
from src.metadata_manager import MetadataManager
from src.config import FUELS_DATA_DIR

class AddAdditionalDataToEachFile:
    def __init__(
        self, 
        df: pd.DataFrame,
        names_of_files_under_procession: Optional[List[str]] = None,
        metadata_manager: Optional[MetadataManager] = None,
        log_manager: Optional[LogManager] = None
    ):
        """
        Class to add additional fuel data to a DataFrame.

        Args:
            df (pd.DataFrame): Input DataFrame.
            names_of_files_under_procession (List[str], optional): Names of files being processed.
            metadata_manager (MetadataManager, optional): Metadata manager instance.
            log_manager (LogManager, optional): Log manager instance.
        """
        self.df = df
        self.names_of_files_under_procession = names_of_files_under_procession
        self.metadata_manager = metadata_manager
        self.log_manager = log_manager

    def add_fuel(self) -> pd.DataFrame:
        """
        Adds fuel properties to the DataFrame based on matching short names in the fuel data JSON.

        Returns:
            pd.DataFrame: Updated DataFrame with additional columns.
        """
        if self.log_manager:
            self.log_manager.info("Starting add_fuel process.")
        
        # Load fuel data
        try:
            with open(FUELS_DATA_DIR, "r") as f:
                fuels_data = json.load(f)
            fuels_data_dict = {fuel["short_name"]: fuel["properties"] for fuel in fuels_data}
        except (FileNotFoundError, json.JSONDecodeError) as e:
            if self.log_manager:
                self.log_manager.error(f"Error reading fuels data: {e}")
            raise

        # Process each row in the DataFrame
        for index, row in self.df.iterrows():
            fuel_short_name = row.get("fuel", "")
            fuel_properties = fuels_data_dict.get(fuel_short_name)
            
            if fuel_properties:
                for prop_name, prop_val in fuel_properties.items():
                    column_name = f"{prop_name}, {prop_val[1]}"
                    self.df.at[index, column_name] = prop_val[0]

        # Construct step_8_file_name
        self.step_8_file_name = self._construct_file_name()

        # Update metadata if applicable
        self._update_metadata()

        # Log final DataFrame details
        self._log_dataframe_details()

        return self.df

    def _construct_file_name(self) -> str:
        """
        Constructs the step_8_file_name based on processed file names.

        Returns:
            str: Constructed file name.
        """
        if self.names_of_files_under_procession and len(self.names_of_files_under_procession) >= 3:
            return (
                f"8-main_file_name:{self.names_of_files_under_procession[0]}, "
                f"eco_file_name:{self.names_of_files_under_procession[1]}, "
                f"Fuel:{self.names_of_files_under_procession[2]}"
            )
        return "8-main_file_name:unknown, eco_file_name:unknown, Fuel:unknown"

    def _update_metadata(self):
        """
        Updates metadata using the metadata manager if available.
        """
        if self.metadata_manager:
            metadata_info = {
                "df.shape": self.df.shape,
                "columns in df": list(self.df.columns),
            }
            self.metadata_manager.update_metadata(
                self.step_8_file_name,
                "Fuel data added successfully.",
                metadata_info,
            )

    def _log_dataframe_details(self):
        """
        Logs details of the DataFrame after processing.
        """
        if self.log_manager:
            self.log_manager.info(f"DataFrame shape after processing: {self.df.shape}")
            self.log_manager.info(f"Columns in DataFrame: {list(self.df.columns)}")
            self.log_manager.info("add_fuel process completed successfully.")


