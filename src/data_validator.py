import pandas as pd
import logging
from typing import List, Dict, Tuple

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

"""
this "class DataValidator:" is refactored form NAWA project:

    parameters_all = ['Ciś. pow. za turb.[Pa]', 'ECT - wyjście z sil.[°C]', 'MAF[kg/h]', 'Moc[kW]', 
                      'Moment obrotowy[Nm]', 'Obroty[obr/min]', 'Temp. oleju w misce[°C]', 
                      'Temp. pal. na wyjściu sil.[°C]', 'Temp. powietrza za turb.[°C]', 
      'Temp. spalin 1/6[°C]', 'Temp. spalin 2/6[°C]', 'Temp. spalin 3/6[°C]', 'Temp. spalin 4/6[°C]',  # count average
                      'Zużycie paliwa średnie[g/s]', 
      'Ciśnienie atmosferyczne[hPa]', 'Temp. otoczenia[°C]', 'Wilgotność względna[%]']

       ........
        self.get_lst_param()
       ........
      
    def get_lst_param(self):
        self.parameters = []
        self.correction = []
        self.lst_columns = list(self.df.columns)
        for par in self.parameters_all:           #!!!!!!!!!!!!!!!!
            if par in self.df.columns:                
                self.index_df_par = self.lst_columns.index(par)
                self.parameters.append(list([self.lst_columns[self.index_df_par - 1], 
                                            self.lst_columns[self.index_df_par]])) 
        print('parameters:', *self.parameters, sep='\n', end='\n\n')

"""

class DataValidator:
    def __init__(self, df: pd.DataFrame, parameters_all: List[str], optional_params: List[str] = []):
        """
        Initialize the DataValidator.

        Parameters:
        - df: DataFrame to validate.
        - parameters_all: List of required parameters.
        - optional_params: List of optional parameters.
        """
        self.df = df
        self.parameters_all = parameters_all
        self.optional_params = optional_params
        self.missing_required = []
        self.missing_optional = []

    def validate_columns(self) -> Dict[str, List[str]]:
        """
        Validate that required and optional columns exist in the DataFrame.

        Returns:
        - A dictionary with validation results.
        """
        logger.info("Starting column validation...")
        self.missing_required = [param for param in self.parameters_all if param not in self.df.columns]
        self.missing_optional = [param for param in self.optional_params if param not in self.df.columns]

        if self.missing_required:
            logger.error(f"Missing required columns: {self.missing_required}")
        if self.missing_optional:
            logger.warning(f"Missing optional columns: {self.missing_optional}")
        else:
            logger.info("All required and optional columns are present.")

        return {
            "missing_required": self.missing_required,
            "missing_optional": self.missing_optional,
            "valid": len(self.missing_required) == 0
        }

    def validate_schema(self, expected_schema: Dict[str, type]) -> Dict[str, List[Tuple[str, type]]]:
        """
        Validate the schema of the DataFrame (column names and types).

        Parameters:
        - expected_schema: A dictionary where keys are column names and values are expected data types.

        Returns:
        - A dictionary with mismatches in column types.
        """
        logger.info("Starting schema validation...")
        mismatches = []
        for column, expected_type in expected_schema.items():
            if column in self.df.columns:
                actual_type = self.df[column].dtype
                if not pd.api.types.is_dtype_equal(actual_type, expected_type):
                    mismatches.append((column, actual_type))
                    logger.warning(f"Column '{column}' type mismatch: Expected {expected_type}, Found {actual_type}")

        if mismatches:
            logger.error("Schema validation failed.")
        else:
            logger.info("Schema validation passed.")

        return {"mismatches": mismatches, "valid": len(mismatches) == 0}

    def get_metadata(self) -> pd.DataFrame:
        """
        Extract metadata for the DataFrame columns (e.g., null counts, unique values).

        Returns:
        - A DataFrame with column metadata.
        """
        logger.info("Extracting metadata...")
        metadata = pd.DataFrame({
            "Column": self.df.columns,
            "Non-Null Count": self.df.notnull().sum().values,
            "Null Count": self.df.isnull().sum().values,
            "Unique Values": [self.df[col].nunique() for col in self.df.columns],
            "Data Type": self.df.dtypes.values
        })
        logger.info("Metadata extraction completed.")
        return metadata

    def generate_report(self) -> str:
        """
        Generate a validation report summarizing the results.

        Returns:
        - A string report summarizing validation outcomes.
        """
        report = []
        report.append("Validation Report")
        report.append("=================")
        if self.missing_required:
            report.append(f"Missing Required Columns: {', '.join(self.missing_required)}")
        else:
            report.append("All Required Columns: Present")
        
        if self.missing_optional:
            report.append(f"Missing Optional Columns: {', '.join(self.missing_optional)}")
        else:
            report.append("All Optional Columns: Present")
        
        return "\n".join(report)

    def run_all_validations(self, expected_schema: Dict[str, type] = None):
        """
        Run all validations and print a summary report.

        Parameters:
        - expected_schema: Schema to validate, if provided.
        """
        self.validate_columns()
        if expected_schema:
            self.validate_schema(expected_schema)
        logger.info(self.generate_report())
