import numpy as np
import pandas as pd
from typing import List
from src.log_manager import LogManager
from src.metadata_manager import MetadataManager
from src.data_cleaner import DataCleaner

class DataTransformation:
    """
    A class to perform data transformations on a DataFrame.
    """

    def __init__(self, df: pd.DataFrame,
                 names_of_files_under_procession: List[str] = None,
                 metadata_manager: MetadataManager = None,
                 log_manager: LogManager = None,
                 data_cleaner: DataCleaner = None):
        """
        Initialize the DataTransformation.

        Parameters:
        - df: The DataFrame to transform.
        - names_of_files_under_procession: A list of file names under procession.
        - metadata_manager: An instance of MetadataManager to handle metadata.
        - log_manager: An instance of LogManager for logging.
        """
        # ...existing code...
        self.df = df
        self.names_of_files_under_procession = names_of_files_under_procession
        self.metadata_manager = metadata_manager
        self.log_manager = log_manager

    
    def atmospheric_power_correction(self, Z: float = 120000.0, displacement: float = 4.5, show_corrections: bool = False) -> pd.DataFrame:
        """
        Corrects the power output of the engine for atmospheric conditions.

        Parameters:
        - Z: A constant used in the calculation (default: 120,000)
        - displacement: Engine displacement in liters (default: 4.5)
        - show_corrections: If True, logs how big the corrections were.

        Returns:
        - The corrected DataFrame.
        """
        # List of required columns for the calculation
        required_columns = [
            'Zużycie paliwa średnie[g/s]',
            'Obroty[obr/min]',
            'Ciś. pow. za turb.[Pa]',
            'Ciśnienie atmosferyczne[hPa]',
            'Temp. otoczenia[°C]',
            'Moc[kW]',
            'Moment obrotowy[Nm]',
        ]

        # Check if all required columns are present
        missing_columns = [col for col in required_columns if col not in self.df.columns]
        if missing_columns:
            message = f"Missing columns for atmospheric power correction: {missing_columns}"
            if self.log_manager:
                self.log_manager.log_error(message)
            else:
                print(message)
            return self.df  # Return the original DataFrame if missing columns

        if self.log_manager:
            self.log_manager.log_info("Starting atmospheric power correction.")

        # Create a DataFrame to store intermediate calculations
        df_cor = pd.DataFrame(index=self.df.index)

        try:
            # Calculate 'q'
            df_cor['q'] = (Z * self.df['Zużycie paliwa średnie[g/s]']) / \
                          (displacement * self.df['Obroty[obr/min]'])

            # Calculate 'r'
            df_cor['r'] = (self.df['Ciś. pow. za turb.[Pa]'] / 10.0) / \
                          self.df['Ciśnienie atmosferyczne[hPa]']

            # Calculate 'qc'
            df_cor['qc'] = df_cor['q'] / df_cor['r']

            # Calculate 'fm' and apply corrections
            df_cor['fm'] = 0.036 * df_cor['qc'] - 1.14
            df_cor['fm'] = np.where(df_cor['qc'] <= 37.2, 0.2, df_cor['fm'])
            df_cor['fm'] = np.where(df_cor['qc'] >= 65.0, 1.2, df_cor['fm'])

            # Calculate 'fa'
            df_cor['fa'] = ((99.0 / (self.df['Ciśnienie atmosferyczne[hPa]'] / 10.0)) ** 0.7) * \
                           (((self.df['Temp. otoczenia[°C]'] + 273.15) / 298.15) ** 1.2)

            # Calculate 'ac'
            df_cor['ac'] = df_cor['fa'] ** df_cor['fm']

            # Calculate corrected power and torque
            df_cor['Pref'] = df_cor['ac'] * self.df['Moc[kW]']
            df_cor['Mref'] = df_cor['ac'] * self.df['Moment obrotowy[Nm]']

            # Optionally, calculate and log corrections
            if show_corrections and self.log_manager:
                power_correction = ((df_cor['Pref'] - self.df['Moc[kW]']) / self.df['Moc[kW]']) * 100.0
                torque_correction = ((df_cor['Mref'] - self.df['Moment obrotowy[Nm]']) / self.df['Moment obrotowy[Nm]']) * 100.0
                avg_power_correction = power_correction.mean()
                max_power_correction = power_correction.max()
                avg_torque_correction = torque_correction.mean()
                max_torque_correction = torque_correction.max()
                self.log_manager.log_info(f"Average power correction: {avg_power_correction:.2f}%")
                self.log_manager.log_info(f"Maximum power correction: {max_power_correction:.2f}%")
                self.log_manager.log_info(f"Average torque correction: {avg_torque_correction:.2f}%")
                self.log_manager.log_info(f"Maximum torque correction: {max_torque_correction:.2f}%")

            # Update 'Moc[kW]' and 'Moment obrotowy[Nm]' with corrected values
            self.df['Moc[kW]'] = df_cor['Pref']
            self.df['Moment obrotowy[Nm]'] = df_cor['Mref']

            # Drop atmospheric parameter columns
            self.df.drop(columns=['Ciśnienie atmosferyczne[hPa]', 'Temp. otoczenia[°C]', 'Wilgotność względna[%]'],
                         inplace=True, errors='ignore')

            if self.log_manager:
                self.log_manager.log_info("Atmospheric power correction applied successfully.")

            # Update metadata if applicable
            self.step_6_file_name = f"5-main_file_name:{self.names_of_files_under_procession[0]}, eco_file_name:{self.names_of_files_under_procession[1]}, Fuel:{self.names_of_files_under_procession[2]}"
            if self.metadata_manager:
                self.metadata_manager.update_metadata(
                    self.step_6_file_name,
                    'Atmospheric power correction applied successfully.',
                    {
                        "df.shape": self.df.shape, 
                        "Max power correction": max_power_correction,
                        "Max torque correction": max_torque_correction,
                        "columns in df": self.df.columns
                    }
                )
  
            return self.df  # Return the corrected DataFrame

        except Exception as e:
            error_message = f"An error occurred during atmospheric power correction: {e}"
            if self.log_manager:
                self.log_manager.log_error(error_message)
            else:
                print(error_message)
            return self.df  # Return the original DataFrame in case of error
         
                    
                    
    def temp_exhaust_gas_mean(self):
        """
        Calculates the mean temperature of the exhaust gas.
        """
        pass
