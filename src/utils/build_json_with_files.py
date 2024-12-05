import os
import json
import logging
import re
from src.config import (
    RAW_DATA_DIR, 
    PROCESSED_DATA_DIR, 
    METADATA_DIR, 
    LOGS_DIR, 
    RAW_PARQUET_DATA_DIR, 
    RAUGH_CSV_DATA_DIR, 
    RAUGH_XLSX_DATA_DIR, 
    RAUGH_PARQUT_DATA_DIR
)
from src.log_manager import LogManager


class JSONBuilder:
    def __init__(self, data_dir, log_manager: LogManager = None):
        self.data_dir = data_dir
        self.template = {
            "id": 0,
            "main_file_name": "",
            "eco_file_name": "",
            "description": "",
            "diesel_test_type": "",
            "fuel": "diesel",
            "diesel_engine_name": "",
        }
        self.json_data = {"Lublin Diesel": []}
        self.current_id = 1  # Initialize the ID counter
        self.log_manager = log_manager
        if self.log_manager:
            self.log_manager.log_info("JSONBuilder initialized.")

    def parse_file_name(self, file_name):
        # Extract test_date, fuel, and test_type from the file name
        date_match = re.search(r'\d{4}-\d{2}-\d{2}|\d{4}-\d{2}', file_name)
        test_date = date_match.group(0) if date_match else ""

        pattern_fuel = re.search(r"""
            (
                ON |                # Matches 'ON'
                B20 |               # Matches 'B20'
                RME |               # Matches 'RME'
                HVO25 |             # Matches 'HVO25'
                HVO |               # Matches 'HVO'
                AG2 |               # Matches 'AG2'
                U75 |               # Matches 'U75'
                BIOW50 |            # Matches 'BIOW50'
                BIOW |              # Matches 'BIOW'
                ONE |               # Matches 'ONE'
                Efecta |            # Matches 'Efecta'
                Efekta\ Agrotronika | # Matches 'Efekta Agrotronika'
                Verwa |             # Matches 'Verwa'
                Verva |             # Matches 'Verva'
                HHO                 # Matches 'HHO'
            )
        """, file_name, re.IGNORECASE | re.VERBOSE)
        fuel = pattern_fuel.group(0) if pattern_fuel else ""

        pattern_test_type = re.compile(r"""
            (
                NRTC |                 # Matches 'NRTC'
                NRTS |                 # Matches 'NRTS'
                TRiL |                 # Matches 'TRiL'
                TMiE |                 # Matches 'TMiE'
                obc\ ?\d{3,4} |        # Matches 'obc 1500', 'obc1500'
                \d{4}\ RPM |           # Matches '1500 RPM'
                \d{4}p?\ rpm |         # Matches '2000p rpm'
                \d{4}\ RPM\ powtórka | # Matches '1900 RPM powtórka'
                \d{4}p?                # Matches '2200', '2200p'
            )
        """, re.IGNORECASE | re.VERBOSE)
        test_type_match = pattern_test_type.search(file_name)
        test_type = test_type_match.group(0) if test_type_match else ""

        return test_date, fuel, test_type

    def build_json(self):
        if self.log_manager:
            self.log_manager.log_info("Building JSON data structure...")
        files_dict = {}
        pattern = re.compile(r'^(.*?)(_eco)?\.(csv|xlsx|parquet)$', re.IGNORECASE)
        
        for file_name in os.listdir(self.data_dir):
            match = pattern.match(file_name)
            if match:
                base_name, eco_suffix, ext = match.groups()
                key = base_name.lower()
                if key not in files_dict:
                    entry = self.template.copy()
                    test_date, fuel, test_type = self.parse_file_name(base_name)
                    entry["test_date"] = test_date
                    entry["fuel"] = fuel if fuel else "diesel"
                    entry["diesel_test_type"] = test_type
                    entry["id"] = self.current_id
                    self.current_id += 1
                    files_dict[key] = entry
                else:
                    entry = files_dict[key]
                
                if eco_suffix:
                    entry["eco_file_name"] = file_name
                else:
                    entry["main_file_name"] = file_name
        
        for entry in files_dict.values():
            self.json_data["Lublin Diesel"].append(entry)
        
        if self.log_manager:
            self.log_manager.log_info("JSON data structure built successfully.")

    def save_json(self, output_file_path):
        # Save the JSON structure to a file
        with open(output_file_path, 'w') as json_file:
            json.dump(self.json_data, json_file, indent=4)
        if self.log_manager:
            self.log_manager.log_info(f"JSON file created at {output_file_path}")

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    logger = logging.getLogger(__name__)

    # Create an instance of LogManager
    log_manager = LogManager(logs_dir=LOGS_DIR)

    # Create an instance of JSONBuilder with log_manager
    # folder_with_files = 'C:/Users/vtaustyka/OneDrive/100 JOB-CAREER-BUSINESS/130 Science/pr. Digital Twin (Diesel Engine)/LUBLIN - grant NAWA-2022, DT-2024/all_csv'
    # builder = JSONBuilder(folder_with_files, log_manager=log_manager)
    directories_for_processing = [RAW_PARQUET_DATA_DIR, RAUGH_CSV_DATA_DIR, RAUGH_XLSX_DATA_DIR, RAUGH_PARQUT_DATA_DIR]
    dir_for_proc = directories_for_processing[0]
    builder = JSONBuilder(dir_for_proc, log_manager=log_manager)

    
    # Build the JSON data
    builder.build_json()
    
    # Define the output file path
    output_file_path = os.path.join(dir_for_proc, 'files_with_raw_data_links.json')
    
    # Save the JSON data to the file
    builder.save_json(output_file_path)