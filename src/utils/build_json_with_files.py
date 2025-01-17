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

chosen_fuels = [
    {"DF": ["ON", "Diesel", "diesel"]},
    {"HVO": ["HVO", "Hydrotreated Vegetable Oil"]},
    {"HVO25": ["HVO25", "25%HVO+75%DF"]},
    {"RME": ["RME", "Rapeseed Methyl Ester"]},
    {"EDF": ["Efecta", "Efekta", "Efekta Agrotronika", "Efecta Diesel Fuel"]},
    {"UCOME": ["BIOW", "UCOME", "Used Cooking Oil Methyl Esters"]},    
]

eliminate_diesel_test_type = ["NRTC", "NRTS"]

#BUMG AG2 – mieszanina oleju napędowego z dodatkiem nanosrebra (2%) rozpuszczonego w wodzie"
eliminate_fuels =["BUMA", "BUMG", "BUMA ON", "BUMG ON" "AG2"]

class JSONBuilder:
    def __init__(self, data_dir, log_manager: LogManager = None):
        self.data_dir = data_dir
        self.template = {
            "id": 0,
            "main_file_name": "empty",
            "eco_file_name": "empty",
            "description": "empty",
            "diesel_test_type": "empty",
            "fuel": "empty",
            "diesel_engine_name": "empty",
        }
        self.json_data = {"Lublin Diesel": []}
        self.current_id = 1  # Initialize the ID counter
        self.log_manager = log_manager
        if self.log_manager:
            self.log_manager.log_info("JSONBuilder initialized.")

    def _parse_file_name(self, file_name):
        # Extract test_date, fuel, and test_type from the file name
        date_match = re.search(r'\d{4}-\d{2}-\d{2}|\d{4}-\d{2}', file_name)
        test_date = date_match.group(0) if date_match else ""

        pattern_fuel = re.search(r"""
            (
                BUMA | BUMG | BUMA ON | BUMG ON | AG2
                ON |                # Matches 'ON'
                B20 |               # Matches 'B20', rapeseed oil methyl esters 20% volume fraction
                RME |               # Matches 'RME'
                HVO25 |             # Matches 'HVO25'
                HVO |               # Matches 'HVO'
                AG2 |               # Matches 'AG2'
                U75 |               # Matches 'U75', methyl esters of waste vegetable oils 75% with diesel oil – 25% volume fraction
                BIOW50 |            # Matches 'BIOW50', Waste cooking oils (WCO) methyl esters
                BIOW |              # Matches 'BIOW', Waste frying oils (WFO) methyl esters
                ONE |               # Matches 'ONE'
                Efecta |            # Matches 'Efecta'
                Efekta\ Agrotronika | # Matches 'Efekta Agrotronika'
                Verwa |             # Matches 'Verwa'
                Verva |             # Matches 'Verva', ulepszonego oleju napędowego o nazwie handlowej Verwa
                HHO                 # Matches 'HHO', dodatku gazu Brauna do kolektora dolotowego, 
                                    # Brown's Gas (HHO) to the intake manifold. 
                                    # HHO is a mixture of 2/3 hydrogen and 1/3 oxygen by volume
            )
        """, file_name, re.IGNORECASE | re.VERBOSE)
        fuel = pattern_fuel.group(0) if pattern_fuel else ""

        pattern_test_type = re.compile(r"""
            (
                zewnętrzna |            # Matches 'zewnętrzna'
                zew |                   # Matches 'zewnętrzna'
                ZEW |                   # Matches 'zewnętrzna'
                NRTC |                 # Matches 'NRTC'
                NRTS |                 # Matches 'NRTS'
                TRiL |                 # Matches 'TRiL' charakterystyka obciążeniowa,
                IRiL |                 # Matches 'IRiL' charakterystyka zewnętrzna,
                TMiE |                 # Matches 'TMiE'
                Tans II |               # Matches 'Tans II' charakterystyka obciążeniowa
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
                    test_date, fuel, test_type = self._parse_file_name(base_name)
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

        return self.json_data

    def save_json(self, output_file_path):
        # Save the JSON structure to a file
        with open(output_file_path, 'w') as json_file:
            json.dump(self.json_data, json_file, indent=4)
        if self.log_manager:
            self.log_manager.log_info(f"JSON file created at {output_file_path}")

    def filter_eliminated_test_types(self):
        self.json_data["Lublin Diesel"] = [
            item for item in self.json_data["Lublin Diesel"]
            if item["diesel_test_type"] not in eliminate_diesel_test_type
        ]

        return self.json_data
    
    def filter_eliminated_fuel_types(self):
        self.json_data["Lublin Diesel"] = [
            item for item in self.json_data["Lublin Diesel"]
            if item["fuel"] not in eliminate_fuels
        ]

        return self.json_data

    def filter_chosen_fuels(self):
        fuel_map = {}
        for group in chosen_fuels:
            for key, fuels in group.items():
                for f in fuels:
                    fuel_map[f.lower()] = key

        filtered = []
        for item in self.json_data["Lublin Diesel"]:
            if item["fuel"].lower() in fuel_map:
                item["fuel"] = fuel_map[item["fuel"].lower()]
                filtered.append(item)
        self.json_data["Lublin Diesel"] = filtered

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
    output_file_path_for_all = os.path.join(dir_for_proc, 'files_with_raw_data_links.json')
    
    # Save the JSON data to the file -- all files
    builder.save_json(output_file_path_for_all)

    # Filter the JSON data
    builder.filter_eliminated_test_types()
    # builder.filter_eliminated_fuel_types()  # dont required
    builder.filter_chosen_fuels()

    # Save the JSON data to the file -- only chosen fuels
    output_file_path_for_chosen = os.path.join(dir_for_proc, 'only_chosen_fuels.json')
    builder.save_json(output_file_path_for_chosen)