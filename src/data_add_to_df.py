import json
import unicodedata
import numpy as np
import pandas as pd
from typing import List, Optional
from src.log_manager import LogManager
from src.metadata_manager import MetadataManager
from src.config import FUELS_DATA_DIR

column_names_from_pl_to_en_full = {
    'Ciś. pow. za turb.[Pa]': ['Air Pressure After Turbo [Pa]', 'Turbo Pressure'],
    'ECT - wyjście z sil.[°C]': ['Engine Coolant Temperature at Engine Outlet [°C]', 'Coolant Temp'],
    'MAF[kg/h]': ['Mass Air Flow [kg/h]', 'MAF'],
    'Moc[kW]': ['Engine Power [kW]', 'Power'],
    'Moment obrotowy[Nm]': ['Engine Torque [Nm]', 'Torque'],
    'Obroty[obr/min]': ['Engine Speed [rpm]', 'RPM'],
    'Temp. oleju w misce[°C]': ['Oil Temperature in Sump [°C]', 'Oil Temp'],
    'Temp. pal. na wyjściu sil.[°C]': ['Fuel Temperature at Engine Outlet [°C]', 'Fuel Temp'],
    'Temp. powietrza za turb.[°C]': ['Air Temperature After Turbo [°C]', 'Turbo Air Temp'],
    'Temp. spalin mean[°C]': ['Exhaust Gas Temperature 1/6 [°C]', 'Exhaust Temp'],
    'Zużycie paliwa średnie[g/s]': ['Average Fuel Consumption [g/s]', 'Fuel Consump'],
    "Cetane number": ['Cetane Number', 'Cetane number'],
    "Density at 15 °C, kg/m3": ['Density at 15 °C', 'Density-15'],
    "Viscosity at 40 °C, mm2/s": ['Viscosity at 40 °C', 'Viscosity-40'],
    "Flash point, °C": ['Flash Point', 'Flash pt'],
    "LHV (Lower Heating Value), MJ/kg": ['LHV (Lower Heating Value)', 'LHV']
}

class AddAdditionalDataToEachFile:
    def __init__(
        self, 
        df: pd.DataFrame,
        names_of_files_under_procession: Optional[List[str]] = None,
        fuels_data: Optional[List[dict]] = None,
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
        self.fuels_data = fuels_data
        self.metadata_manager = metadata_manager
        self.log_manager = log_manager

    def add_fuel(self) -> pd.DataFrame:
        """
        Adds fuel properties to the DataFrame based on matching fuel name in the given fuel data.

        Returns:
            pd.DataFrame: Updated DataFrame with additional columns containing fuel properties.
        """
        if self.log_manager:
            self.log_manager.log_info("Starting add_fuel process.")

        # Identify the fuel name from names_of_files_under_procession
        if not self.names_of_files_under_procession or len(self.names_of_files_under_procession) < 3:
            if self.log_manager:
                self.log_manager.log_error("Fuel name not found in names_of_files_under_procession.")
            raise ValueError("Fuel name is not provided or invalid in names_of_files_under_procession.")
        
        fuel_name = self.names_of_files_under_procession[2]

        # Match the fuel_name with fuels_data
        matched_fuel_data = next((fuel for fuel in self.fuels_data if fuel["short_name"] == fuel_name), None)
        if not matched_fuel_data:
            if self.log_manager:
                self.log_manager.log_error(f"Fuel '{fuel_name}' not found in fuels_data.")
            raise ValueError(f"Fuel '{fuel_name}' not found in fuels_data.")

        # Extract properties from the matched fuel data
        fuel_properties = matched_fuel_data["properties"]

        # Add new columns for fuel properties
        for prop_name, prop_val in fuel_properties.items():
            column_name = f"{prop_name}, {prop_val[1]}" if prop_val[1] != "-" else prop_name
            self.df[column_name] = prop_val[0]  # Assign the same value for all rows

        # Log the update
        if self.log_manager:
            self.log_manager.log_info(
                f"Fuel properties for '{fuel_name}' added to DataFrame. Columns added: {list(fuel_properties.keys())}"
            )
        self.step_8_file_name = self._construct_file_name()
        self._update_metadata()
        self._log_dataframe_details(name_of_the_function='add_fuel process completed successfully.')

        # Return the updated DataFrame
        return self.df
    
    def _normalize_str(self, s: str) -> str:
        s = s.replace("Â", "")
        return unicodedata.normalize('NFKD', s).encode('ASCII', 'ignore').decode('ASCII').strip()

    def rename_polish_columns_to_english(self, use_full_en_column_name: bool = False) -> pd.DataFrame:
        """
        Renames the DataFrame's columns from Polish to English using the global dictionary.
        Args:
            use_full_en_column_name (bool): If True, use the full descriptive names; if False, use the simplified names.
        Returns:
            pd.DataFrame: DataFrame with updated column names.
        """
        # Create a normalized mapping dictionary
        normalized_mapping = {
            self._normalize_str(k): (v[0] if use_full_en_column_name else v[1])
            for k, v in column_names_from_pl_to_en_full.items()
        }
        
        # Build a new mapping by normalizing the DataFrame's columns as well
        mapping = {}
        for col in self.df.columns:
            norm_col = self._normalize_str(col)
            if norm_col in normalized_mapping:
                mapping[col] = normalized_mapping[norm_col]
        
        self.df = self.df.rename(columns=mapping)
        
        naming = "full" if use_full_en_column_name else "simplified"
        message = f"Columns renamed from Polish to English using {naming} naming."
        self._log_dataframe_details(name_of_the_function=message)
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

    def _log_dataframe_details(self, name_of_the_function: str = "was forgotten") -> None:
        """
        Logs details of the DataFrame after processing.
        """
        if self.log_manager:
            self.log_manager.log_info(f"DataFrame shape after processing: {self.df.shape}")
            self.log_manager.log_info(f"Columns in DataFrame: {list(self.df.columns)}")
            self.log_manager.log_info(name_of_the_function)

    


