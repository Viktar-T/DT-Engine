import pandas as pd
import logging
from typing import List, Dict, Tuple, Union
from pandas.api.types import is_numeric_dtype, is_string_dtype
from tabulate import tabulate
from src.log_manager import LogManager
from src.metadata_manager import MetadataManager

# logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
# logger = logging.getLogger(__name__)


class DataValidator:
    def __init__(self, dfs: List[pd.DataFrame], 
                 required_columns_list: List[List[str]], 
                 optional_columns_list: List[List[str]] = [], 
                 file_names: List[str] = [],
                 names_of_files_under_procession: List[str] = None,
                 log_manager: LogManager = None,
                 metadata_manager: MetadataManager = None):
        """
        Initialize the DataValidator.

        Parameters:
        - dfs: List of DataFrames to validate.
        - required_columns_list: List of required columns for each DataFrame.
        - optional_columns_list: List of optional columns for each DataFrame.
        - file_names: List of file names for each DataFrame.
        - names_of_files_under_procession: List of file names under procession.
        - log_manager: An instance of LogManager for logging.
        """
        self.dfs = dfs
        self.required_columns_list = required_columns_list
        self.names_of_files_under_procession = names_of_files_under_procession
        self.optional_columns_list = optional_columns_list or [[] for _ in dfs]
        self.file_names = file_names or [f"DataFrame_{i}" for i in range(len(dfs))]   # !!! duplication
        self.missing_required_list = []
        self.missing_optional_list = []
        self.metadata_manager = metadata_manager
        if self.metadata_manager:
            self.step_4_file_name = f"4-main_file_name:{self.names_of_files_under_procession[0]}, eco_file_name:{self.names_of_files_under_procession[1]}, Fuel:{self.names_of_files_under_procession[2]}"
            self.metadata_manager.update_metadata(self.step_4_file_name, 
                                                  'from class DataValidator. Files for validation:', 
                                                  self.file_names)
        
        self.log_manager = log_manager
        if self.log_manager:
            self.log_manager.log_info("DataValidator initialized.")

    def validate_columns(self) -> List[Dict[str, Union[List[str], bool]]]:
        if self.log_manager:
            self.log_manager.log_info("Starting column validation for multiple DataFrames...")
        results = []
        for idx, df in enumerate(self.dfs):
            missing_required = [col for col in self.required_columns_list[idx] if col not in df.columns]
            missing_optional = [col for col in self.optional_columns_list[idx] if col not in df.columns]
            if missing_required and self.log_manager:
                self.log_manager.log_error(f"DataFrame {idx}: Missing required columns: {missing_required}")
            elif self.log_manager:
                self.log_manager.log_info(f"DataFrame {idx}: All required columns are present.")
            if missing_optional and self.log_manager:
                self.log_manager.log_warning(f"DataFrame {idx}: Missing optional columns: {missing_optional}")
            elif self.log_manager:
                self.log_manager.log_info(f"DataFrame {idx}: All optional columns are present.")
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
        if self.log_manager:
            self.log_manager.log_info("Starting schema validation for multiple DataFrames...")
        results = []
        for idx, (df, expected_schema) in enumerate(zip(self.dfs, expected_schemas)):
            mismatches = []
            for column, expected_type in expected_schema.items():
                if column in df.columns:
                    actual_dtype = df[column].dtype
                    if expected_type == 'numeric' and not is_numeric_dtype(df[column]):
                        mismatches.append((column, str(actual_dtype)))
                        self.log_manager.log_warning(f"DataFrame {idx} - Column '{column}' expected to be numeric, found {actual_dtype}")
                    elif expected_type == 'string' and not is_string_dtype(df[column]):
                        mismatches.append((column, str(actual_dtype)))
                        self.log_manager.log_warning(f"DataFrame {idx} - Column '{column}' expected to be string, found {actual_dtype}")
            if mismatches:
                self.log_manager.log_error(f"DataFrame {idx} - Schema validation failed.")
            else:
                self.log_manager.log_info(f"DataFrame {idx} - Schema validation passed.")
            results.append({"mismatches": mismatches, "valid": len(mismatches) == 0})
        return results

    def handle_missing_columns(self, fill_value=0):
        for idx, df in enumerate(self.dfs):
            if self.missing_required_list[idx]:
                for column in self.missing_required_list[idx]:
                    df[column] = fill_value
                    if self.log_manager:
                        self.log_manager.log_info(f"DataFrame {idx}: Filled missing required column '{column}' with {fill_value}")

    def check_for_duplicate_columns(self):
        if self.log_manager:
            self.log_manager.log_info("Checking for duplicate columns in multiple DataFrames...")
        for idx, df in enumerate(self.dfs):
            duplicates = df.columns[df.columns.duplicated()].tolist()
            if duplicates:
                self.log_manager.log_warning(f"DataFrame {idx}: Duplicate columns found: {duplicates}")
            else:
                self.log_manager.log_info(f"DataFrame {idx}: No duplicate columns found.")

    def get_metadata(self, data_frames=None, message_for_logs=None) -> List[pd.DataFrame]:
        if data_frames is None:
            data_frames = self.dfs
        else:
            self.message_for_logs = message_for_logs

        if self.log_manager:
            self.log_manager.log_info("Starting metadata extraction for multiple DataFrames...")
        metadata_list = []

        for idx, df in enumerate(data_frames):
            if self.log_manager:
                self.log_manager.log_info(f"Processing DataFrame {idx + 1}/{len(data_frames)} with shape {df.shape}...")

            metadata = pd.DataFrame({
                "Column": df.columns,
                "Total Values": df.count().values,
                "Non-Null Count": df.notnull().sum().values,
                "Null Count": df.isnull().sum().values,
                "Unique Values": [df[col].nunique() for col in df.columns],
                "Data Type": df.dtypes.values
            })
            metadata_list.append(metadata)
            if self.log_manager:
                self.log_manager.log_info(f"Metadata for DataFrame {idx}, names_of_files_under_procession:{self.names_of_files_under_procession}, {message_for_logs}:\n{tabulate(metadata, headers='keys', tablefmt='grid')}")

        if self.log_manager:
            self.log_manager.log_info("Metadata extraction completed for all DataFrames.")
        return metadata_list

    def generate_report(self) -> List[str]:
        if self.log_manager:
            self.log_manager.log_info("Validation reports summarizing the results for each DataFrame")
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
        self.validate_columns()
        if expected_schemas:
            self.validate_schema(expected_schemas)
        self.check_for_duplicate_columns()
        reports = self.generate_report()
        for report in reports:
            if self.log_manager:
                self.log_manager.log_info(report)

    def validate_schema_for_df(self, idx: int, expected_schema: Dict[str, Union[type, str]]) -> Dict[str, Union[List[Tuple[str, str]], bool]]:
        if self.log_manager:
            self.log_manager.log_info(f"Starting schema validation for DataFrame {idx}...")
        mismatches = []
        df = self.dfs[idx]
        for column, expected_type in expected_schema.items():
            if column in df.columns:
                actual_dtype = df[column].dtype
                if expected_type == 'numeric' and not is_numeric_dtype(df[column]):
                    mismatches.append((column, str(actual_dtype)))
                    self.log_manager.log_warning(f"DataFrame {idx} - Column '{column}' expected to be numeric, found {actual_dtype}")
                elif expected_type == 'string' and not is_string_dtype(df[column]):
                    mismatches.append((column, str(actual_dtype)))
                    self.log_manager.log_warning(f"DataFrame {idx} - Column '{column}' expected to be string, found {actual_dtype}")
        if mismatches:
            self.log_manager.log_error(f"DataFrame {idx} - Schema validation failed.")
        else:
            self.log_manager.log_info(f"DataFrame {idx} - Schema validation passed.")
        return {"mismatches": mismatches, "valid": len(mismatches) == 0}
