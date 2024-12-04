import os
import json
import logging
import re
from src.config import RAW_DATA_DIR, LOGS_DIR
from src.log_manager import LogManager


class JSONBuilder:
    def __init__(self, data_dir, log_manager: LogManager = None):
        self.data_dir = data_dir
        self.template = {
            "id": 0,
            "main_file_name": "",
            "eco_file_name": "",
            "description": "empty",
            "diesel_test_type": "",
            "fuel": "diesel",
            "diesel_engine_name": "empty",
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
                Efekta\ Agrotronika | # Matches 'Efekta Agrotronika'
                HVO |               # Matches 'HVO'
                HVO25 |             # Matches 'HVO25'
                AG2 |               # Matches 'AG2'
                U75 |               # Matches 'U75'
                BIOW |              # Matches 'BIOW'
                BIOW50 |            # Matches 'BIOW50'
                ONE |               # Matches 'ONE'
                Efecta |            # Matches 'Efecta'
                Verwa |             # Matches 'Verwa'
                Verva |             # Matches 'Verva'
                HHO                 # Matches 'HHO'
            )
        """, file_name, re.IGNORECASE | re.VERBOSE)
        fuel = pattern_fuel.group(0) if pattern_fuel else ""

        pattern_test_type = re.compile(r"""
            (
                1600Nn\ obc |  # Matches '1600Nn obc'
                Zew |          # Matches 'Zew'
                zew |          # Matches 'zew'
                NRTC |         # Matches 'NRTC'
                obc\ 1600 |    # Matches 'obc 1600'
                obc\ 1500 |    # Matches 'obc 1500'
                obc\ 2000 |    # Matches 'obc 2000'
                obc\ 2400 |    # Matches 'obc 2400'
                obc1600 |      # Matches 'obc1600'
                obc1500 |      # Matches 'obc1500'
                obc2000 |      # Matches 'obc2000'
                obc2400 |      # Matches 'obc2400'
                1500\ RPM |    # Matches '1500 RPM'
                1300\ RPM |    # Matches '1300 RPM'
                1700\ RPM |    # Matches '1700 RPM'
                1900\ RPM |    # Matches '1900 RPM'
                2100\ RPM |    # Matches '2100 RPM'
                2200\ RPM      # Matches '2200 RPM'
            )
        """, re.IGNORECASE | re.VERBOSE)
        test_type_match = pattern_test_type.search(file_name)
        test_type = test_type_match.group(0) if test_type_match else ""

        return test_date, fuel, test_type

    def build_json(self):
        if self.log_manager:
            self.log_manager.log_info("Building JSON data structure...")
        # Scan the directory for .csv, .xlsx, and .parquet files
        for file_name in os.listdir(self.data_dir):
            if file_name.endswith('.csv') or file_name.endswith('.xlsx') or file_name.endswith('.parquet'):
                if self.log_manager:
                    self.log_manager.log_info(f"Processing file: {file_name}")
                # Create a new entry based on the template
                entry = self.template.copy()
                test_date, fuel, test_type = self.parse_file_name(file_name)
                entry["test_date"] = test_date
                entry["fuel"] = fuel
                entry["diesel_test_type"] = test_type

                if "eco" in file_name.lower():
                    entry["eco_file_name"] = file_name
                else:
                    entry["main_file_name"] = file_name

                # Check if an entry with the same test_date, fuel, and test_type already exists
                existing_entry = next((item for item in self.json_data["Lublin Diesel"]
                                       if item["test_date"] == test_date and
                                          item["fuel"] == fuel and
                                          item["diesel_test_type"] == test_type), None)
                if existing_entry:
                    if "eco" in file_name.lower():
                        existing_entry["eco_file_name"] = file_name
                    else:
                        existing_entry["main_file_name"] = file_name
                else:
                    # Assign a new ID to the entry
                    entry["id"] = self.current_id
                    self.current_id += 1
                    # Add the entry to the JSON structure
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
    builder = JSONBuilder(RAW_DATA_DIR, log_manager=log_manager)

    
    # Build the JSON data
    builder.build_json()
    
    # Define the output file path
    output_file_path = os.path.join(RAW_DATA_DIR, 'files_with_raw_data_links.json')
    
    # Save the JSON data to the file
    builder.save_json(output_file_path)