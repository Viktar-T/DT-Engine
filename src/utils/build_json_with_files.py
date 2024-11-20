import os
import json
import logging
import re
from src.config import RAW_DATA_DIR

class JSONBuilder:
    def __init__(self, data_dir):
        self.data_dir = data_dir
        self.template = {
            "main_file_name": "",
            "eco_file_name": "",
            "description": "empty",
            "diesel_test_type": "",
            "fuel": "diesel",
            "diesel_engine_name": "empty",
            "test_date": "",
        }
        self.json_data = {"Lublin Diesel": []}

    def parse_file_name(self, file_name):
        # Extract test_date, fuel, and test_type from the file name
        date_match = re.search(r'\d{4}-\d{2}-\d{2}', file_name)
        test_date = date_match.group(0) if date_match else ""

        fuel_match = re.search(r'(ON|B20|RME|Efekta Agrotronika)', file_name, re.IGNORECASE)
        fuel = fuel_match.group(0) if fuel_match else ""

        test_type_match = re.search(r'(1600Nn obc|Zew|zew)', file_name, re.IGNORECASE)
        test_type = test_type_match.group(0) if test_type_match else ""

        return test_date, fuel, test_type

    def build_json(self):
        # Scan the directory for .csv and .xlsx files
        for file_name in os.listdir(self.data_dir):
            if file_name.endswith('.csv') or file_name.endswith('.xlsx'):
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
                    # Add the entry to the JSON structure
                    self.json_data["Lublin Diesel"].append(entry)

    def save_json(self, output_file_path):
        # Save the JSON structure to a file
        with open(output_file_path, 'w') as json_file:
            json.dump(self.json_data, json_file, indent=4)
        logging.info(f"JSON file created at {output_file_path}")

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    logger = logging.getLogger(__name__)

    # Create an instance of JSONBuilder
    builder = JSONBuilder(RAW_DATA_DIR)
    
    # Build the JSON data
    builder.build_json()
    
    # Define the output file path
    output_file_path = os.path.join(RAW_DATA_DIR, 'files_with_raw_data_links.json')
    
    # Save the JSON data to the file
    builder.save_json(output_file_path)