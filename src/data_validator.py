import pandas as pd
import logging
from typing import List, Dict, Tuple, Union
from pandas.api.types import is_numeric_dtype, is_string_dtype
from tabulate import tabulate

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
    def __init__(self, dfs: List[pd.DataFrame], 
                 required_columns_list: List[List[str]], 
                 optional_columns_list: List[List[str]] = [], 
                 file_names: List[str] = []):
        """
        Initialize the DataValidator.

        Parameters:
        - dfs: List of DataFrames to validate.
        - required_columns_list: List of required columns for each DataFrame.
        - optional_columns_list: List of optional columns for each DataFrame.
        - file_names: List of file names for each DataFrame.
        """
        self.dfs = dfs
        self.required_columns_list = required_columns_list
        self.optional_columns_list = optional_columns_list or [[] for _ in dfs]
        self.file_names = file_names or [f"DataFrame_{i}" for i in range(len(dfs))]
        self.missing_required_list = []
        self.missing_optional_list = []

    def validate_columns(self) -> List[Dict[str, Union[List[str], bool]]]:
        """
        Validate that required and optional columns exist in each DataFrame.

        Returns:
        - A list of dictionaries with validation results for each DataFrame.
        """
        logger.info("Starting column validation for multiple DataFrames...")
        results = []
        for idx, df in enumerate(self.dfs):
            missing_required = [col for col in self.required_columns_list[idx] if col not in df.columns]
            missing_optional = [col for col in self.optional_columns_list[idx] if col not in df.columns]
            if missing_required:
                logger.error(f"DataFrame {idx}: Missing required columns: {missing_required}")
            else:
                logger.info(f"DataFrame {idx}: All required columns are present.")
            if missing_optional:
                logger.warning(f"DataFrame {idx}: Missing optional columns: {missing_optional}")
            else:
                logger.info(f"DataFrame {idx}: All optional columns are present.")
            result = {
                "missing_required": missing_required,
                "missing_optional": missing_optional,
                "valid": len(missing_required) == 0
            }
            self.missing_required_list.append(missing_required)
            self.missing_optional_list.append(missing_optional)
            results.append(result)
        return results

    def validate_schema(self, expected_schemas: List[Dict[str, Union[type, str]]]) -> List[Dict[str, Union[List[Tuple[str, str]], bool]]]:
        """
        Validate the data types of specified columns for each DataFrame.
        
        Parameters:
        - expected_schemas: A list of dictionaries mapping column names to expected data types for each DataFrame.
        
        Returns:
        - A list of dictionaries with mismatches in column types for each DataFrame.
        """
        logger.info("Starting schema validation for multiple DataFrames...")
        results = []
        for idx, (df, expected_schema) in enumerate(zip(self.dfs, expected_schemas)):
            mismatches = []
            for column, expected_type in expected_schema.items():
                if column in df.columns:
                    actual_dtype = df[column].dtype
                    if expected_type == 'numeric' and not is_numeric_dtype(df[column]):
                        mismatches.append((column, str(actual_dtype)))
                        logger.warning(f"DataFrame {idx} - Column '{column}' expected to be numeric, found {actual_dtype}")
                    elif expected_type == 'string' and not is_string_dtype(df[column]):
                        mismatches.append((column, str(actual_dtype)))
                        logger.warning(f"DataFrame {idx} - Column '{column}' expected to be string, found {actual_dtype}")
            if mismatches:
                logger.error(f"DataFrame {idx} - Schema validation failed.")
            else:
                logger.info(f"DataFrame {idx} - Schema validation passed.")
            results.append({"mismatches": mismatches, "valid": len(mismatches) == 0})
        return results

    def handle_missing_columns(self, fill_value=0):
        """
        Handle missing required columns by filling them with a default value in each DataFrame.

        Parameters:
        - fill_value: The value to fill in missing columns.
        """
        for idx, df in enumerate(self.dfs):
            if self.missing_required_list[idx]:
                for column in self.missing_required_list[idx]:
                    df[column] = fill_value
                    logger.info(f"DataFrame {idx}: Filled missing required column '{column}' with {fill_value}")

    def check_for_duplicate_columns(self):
        """
        Check for duplicate columns in each DataFrame.
        """
        logger.info("Checking for duplicate columns in multiple DataFrames...")
        for idx, df in enumerate(self.dfs):
            duplicates = df.columns[df.columns.duplicated()].tolist()
            if duplicates:
                logger.warning(f"DataFrame {idx}: Duplicate columns found: {duplicates}")
            else:
                logger.info(f"DataFrame {idx}: No duplicate columns found.")

    def get_metadata(self) -> List[pd.DataFrame]:
        """
        Extract metadata for each DataFrame.
        
        Returns:
        - A list of DataFrames with column metadata for each DataFrame.
        """
        logger.info("Starting metadata extraction for multiple DataFrames...")
        metadata_list = []
        
        for idx, df in enumerate(self.dfs):
            logger.info(f"Processing DataFrame {idx + 1}/{len(self.dfs)} with shape {df.shape}...")
            
            metadata = pd.DataFrame({
                "Column": df.columns,
                "Non-Null Count": df.notnull().sum().values,
                "Null Count": df.isnull().sum().values,
                "Unique Values": [df[col].nunique() for col in df.columns],
                "Data Type": df.dtypes.values
            })
            #"File Name": [self.file_names[idx]] * len(df.columns),
            metadata_list.append(metadata)
            logger.info(f"Metadata for DataFrame {idx + 1}, File:{self.file_names}:\n{tabulate(metadata, headers='keys', tablefmt='grid')}")
        
        logger.info("Metadata extraction completed for all DataFrames.")
        return metadata_list

    def generate_report(self) -> List[str]:
        """
        Generate validation reports summarizing the results for each DataFrame,
        including the shape of each DataFrame.

        Returns:
        - A list of string reports summarizing validation outcomes for each DataFrame.
        """
        logger.info("Validation reports summarizing the results for each DataFrame")
        reports = []
        for idx, df in enumerate(self.dfs):
            report = []
            report.append("=================")
            report.append(f"Validation Report for DataFrame {idx} (0 - main Data Frame; 1 - eco Data Frame):")
            # Include the shape of the DataFrame
            report.append(f"DataFrame Shape: {df.shape} (Rows, Columns) <--- !!!!!!!!!")
            missing_required = self.missing_required_list[idx]
            missing_optional = self.missing_optional_list[idx]
            if missing_required:
                report.append(f"Missing Required Columns: {', '.join(missing_required)}")
            else:
                report.append("All Required Columns: Present")
            if missing_optional:
                report.append(f"Missing Optional Columns: {', '.join(missing_optional)}")
            else:
                report.append("All Optional Columns: Present")
            reports.append("\n".join(report))
        return reports

    def run_all_validations(self, expected_schemas: List[Dict[str, Union[type, str]]] = None):
        """
        Run all validations and log a summary report for each DataFrame.

        Parameters:
        - expected_schemas: List of schemas to validate, one for each DataFrame.
        """
        self.validate_columns()
        if expected_schemas:
            self.validate_schema(expected_schemas)
        self.check_for_duplicate_columns()
        reports = self.generate_report()
        for report in reports:
            logger.info(report)

    def validate_schema_for_df(self, idx: int, expected_schema: Dict[str, Union[type, str]]) -> Dict[str, Union[List[Tuple[str, str]], bool]]:
        """
        Validate the data types of specified columns for a specific DataFrame.

        Parameters:
        - idx: Index of the DataFrame in self.dfs.
        - expected_schema: Expected schema for the DataFrame.

        Returns:
        - A dictionary with mismatches in column types.
        """
        logger.info(f"Starting schema validation for DataFrame {idx}...")
        mismatches = []
        df = self.dfs[idx]
        for column, expected_type in expected_schema.items():
            if column in df.columns:
                actual_dtype = df[column].dtype
                if expected_type == 'numeric' and not is_numeric_dtype(df[column]):
                    mismatches.append((column, str(actual_dtype)))
                    logger.warning(f"DataFrame {idx} - Column '{column}' expected to be numeric, found {actual_dtype}")
                elif expected_type == 'string' and not is_string_dtype(df[column]):
                    mismatches.append((column, str(actual_dtype)))
                    logger.warning(f"DataFrame {idx} - Column '{column}' expected to be string, found {actual_dtype}")
        if mismatches:
            logger.error(f"DataFrame {idx} - Schema validation failed.")
        else:
            logger.info(f"DataFrame {idx} - Schema validation passed.")
        return {"mismatches": mismatches, "valid": len(mismatches) == 0}
