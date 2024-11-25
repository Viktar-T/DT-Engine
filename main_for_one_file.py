import logging
import os
import pandas as pd
from datetime import datetime
from src.data_loader import DataLoader
from src.data_validator import DataValidator
from src.data_cleaner import DataCleaner
from src.config import RAW_DATA_DIR, PROCESSED_DATA_DIR, METADATA_DIR, LOGS_DIR
from src.utils.build_json_with_files import JSONBuilder
from src.metadata_manager import MetadataManager
from src.log_manager import LogManager

required_columns_for_validation_step = [
    'Ciś. pow. za turb.[Pa]', 'Ciśnienie atmosferyczne[hPa]', 'ECT - wyjście z sil.[°C]', 'MAF[kg/h]', 'Moc[kW]', 
    'Moment obrotowy[Nm]', 'Obroty[obr/min]', 'Temp. oleju w misce[°C]', 'Temp. otoczenia[°C]',  
    'Temp. pal. na wyjściu sil.[°C]', 'Temp. powietrza za turb.[°C]',
    'Temp. spalin 1/6[°C]', 'Temp. spalin 2/6[°C]', 'Temp. spalin 3/6[°C]', 'Temp. spalin 4/6[°C]', 
    'Wilgotność względna[%]', 'Zużycie paliwa średnie[g/s]'
]

required_columns = [
    ['Czas [ms].1', 'Ciś. pow. za turb.[Pa]'],
    ['Czas [ms].2', 'Ciśnienie atmosferyczne[hPa]'],
    ['Czas [ms].11', 'ECT - wyjście z sil.[°C]'],
    ['Czas [ms].25', 'MAF[kg/h]'],
    ['Czas [ms].26', 'Moc[kW]'],
    ['Czas [ms].27', 'Moment obrotowy[Nm]'],
    ['Czas [ms].29', 'Obroty[obr/min]'],
    ['Czas [ms].46', 'Temp. oleju w misce[°C]'],
    ['Czas [ms].48', 'Temp. otoczenia[°C]'],
    ['Czas [ms].50', 'Temp. pal. na wyjściu sil.[°C]'],
    ['Czas [ms].55', 'Temp. powietrza za turb.[°C]'],
    ['Czas [ms].56', 'Temp. spalin 1/6[°C]'],
    ['Czas [ms].57', 'Temp. spalin 2/6[°C]'],
    ['Czas [ms].58', 'Temp. spalin 3/6[°C]'],
    ['Czas [ms].59', 'Temp. spalin 4/6[°C]'],
    ['Czas [ms].64', 'Wilgotność względna[%]'],
    ['Czas [ms].72', 'Zużycie paliwa średnie[g/s]'],    
]

current_file = {
    "id": 4,
    "main_file_name": "1600Nn obc ON _ 2018-12-06_ORIGEN.csv",
    "eco_file_name": "",
    "description": "empty",
    "diesel_test_type": "1600Nn obc",
    "fuel": "ON",
    "diesel_engine_name": "empty",
    "test_date": "2018-12-06"
}

required_columns_eco = ["OBR", "Mo", "CO", "HC", "LAMBDA", "CO2", "O2", "NO", "PM"]

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def proceed_to_next_step(step_number):
    response = input(f"Step {step_number} was completed. Press [y/n] to proceed: ")
    if response.lower() != 'y':
        logger.info(f"Process stopped at step {step_number}.")
        exit()

def main():
    try:
        # Initialize LogManager
        log_manager = LogManager(logs_dir=LOGS_DIR)
        log_manager.log_info("Starting data pipeline...")

        # Step 1: Build files_with_raw_data_links.json
        metadata_manager = MetadataManager(metadata_dir=METADATA_DIR)
        metadata_manager.update_metadata('1-Build files_with_raw_data_links.json', 'pipeline_status', 'started')
        metadata_manager.update_metadata('1-Build files_with_raw_data_links.json', 'step', '1')
        metadata_manager.update_metadata('1-Build files_with_raw_data_links.json', 'step_name', 'Build files_with_raw_data_links.json')
        metadata_manager.update_metadata('1-Build files_with_raw_data_links.json', 'start_time', str(datetime.now()))

        logger.info("Step 1: Building files_with_raw_data_links.json...")
        builder = JSONBuilder(RAW_DATA_DIR)
        builder.build_json()
        output_file_path = os.path.join(RAW_DATA_DIR, 'files_with_raw_data_links.json')
        builder.save_json(output_file_path)
        logger.info("Step 1: files_with_raw_data_links.json built successfully.")
        metadata_manager.update_metadata("1-Build files_with_raw_data_links.json", 'step_1_status', 'completed')
        metadata_manager.update_metadata("1-Build files_with_raw_data_links.json", 'step_1_end_time', str(datetime.now()))
        proceed_to_next_step(1)
        log_manager.log_info("Step 1: files_with_raw_data_links.json built successfully.")

        # Step 2: Load raw data
        step_2_file_name = "2-raw file_name"
        logger.info("Continue data pipeline. Step 2: Loading raw data...")
        metadata_manager.update_metadata(step_2_file_name, 'step', '2')
        metadata_manager.update_metadata(step_2_file_name, 'step_name', 'Load raw data')
        metadata_manager.update_metadata(step_2_file_name, 'step_2_start_time', str(datetime.now()))
        data_loader = DataLoader(RAW_DATA_DIR, metadata_manager=metadata_manager)
        raw_data_frames = data_loader.select_from_json_and_load_data(selected_id=current_file["id"])
        metadata_manager.update_metadata(step_2_file_name, 'step_2_status', 'completed')
        metadata_manager.update_metadata(step_2_file_name, 'step_2_end_time', str(datetime.now()))
        proceed_to_next_step(2)
        log_manager.log_info("Step 2: Raw data loaded successfully.")

        # Step 3: Validate data
        step_3_file_name = "3-raw file_name"
        logger.info("Continue data pipeline. Step 3: Validating data...")
        metadata_manager.update_metadata(step_3_file_name, 'step', '3')
        metadata_manager.update_metadata(step_3_file_name, 'step_name', 'Validate data')
        metadata_manager.update_metadata(step_3_file_name, 'step_3_start_time', str(datetime.now()))
        required_columns_list = [required_columns_for_validation_step, required_columns_eco]
        validator = DataValidator(raw_data_frames, required_columns_list=required_columns_list, 
                                  file_names=[current_file["main_file_name"], current_file["eco_file_name"]])
        validation_results = validator.validate_columns()
        for idx, result in enumerate(validation_results):
            if not result["valid"]:
                # Handle missing columns for each DataFrame
                validator.handle_missing_columns(fill_value=0)
                logger.info(f"DataFrame {idx}: Missing required columns filled with default values.")

        # Schema validation
        expected_schemas = [
            {col: 'numeric' for col in required_columns_for_validation_step},       # Expected schema for first DataFrame
            {col: 'numeric' for col in required_columns_eco}    # Expected schema for second DataFrame
        ]
        validator.validate_schema(expected_schemas)
        validator.check_for_duplicate_columns()
        reports = validator.generate_report()
        for report in reports:
            logger.info(report)
        # metadata_manager.update_metadata(step_3_file_name, 'validation_reports', reports)
        logger.info("Step 3: Data validated successfully.")
        metadata_manager.update_metadata(step_3_file_name, 'validation_results', validation_results)
        metadata_manager.update_metadata(step_3_file_name, 'step_3_status', 'completed')
        metadata_manager.update_metadata(step_3_file_name, 'step_3_end_time', str(datetime.now()))
        proceed_to_next_step(3)
        log_manager.log_info("Step 3: Data validated successfully.")

        # Step 4: Validate data
        step_4_file_name = "4-raw file_name"
        metadata_list = validator.get_metadata()
        #for idx, metadata in enumerate(metadata_list):
        #    logger.info(f"Metadata for DataFrame {idx}: {metadata}")
        # metadata_manager.update_metadata(step_4_file_name, 'metadata_list', )
        metadata_manager.update_metadata(step_4_file_name, 'step_4_status', 'completed')
        metadata_manager.update_metadata(step_4_file_name, 'step_4_end_time', str(datetime.now()))
        proceed_to_next_step(4)
        log_manager.log_info("Step 4: Metadata extracted successfully.")

        # Step 5: Clean and preprocess data
        step_5_file_name = "5-raw file_name"
        logger.info("Continue data pipeline. Step 5: Cleaning and preprocessing data...")
        metadata_manager.update_metadata(step_5_file_name, 'step', '5')
        metadata_manager.update_metadata(step_5_file_name, 'step_name', 'Clean and preprocess data')
        metadata_manager.update_metadata(step_5_file_name, 'step_5_start_time', str(datetime.now()))
        data_cleaner = DataCleaner(df=raw_data_frames[0], required_columns=required_columns, metadata_manager=metadata_manager)
        cleaned_df = data_cleaner.clean()
        logger.info("Step 5: Data cleaned and preprocessed successfully.")
        metadata_manager.update_metadata(step_5_file_name, 'step_5_status', 'completed')
        metadata_manager.update_metadata(step_5_file_name, 'step_5_end_time', str(datetime.now()))
        proceed_to_next_step(5)
        log_manager.log_info("Step 5: Data cleaned and preprocessed successfully.")

        # Step 6: Save cleaned data
        step_6_file_name = "6-raw file_name"
        logger.info("Step 6: Continue data pipeline. Saving cleaned data...")
        metadata_manager.update_metadata(step_6_file_name, 'step', '6')
        metadata_manager.update_metadata(step_6_file_name, 'step_name', 'Save cleaned data')
        metadata_manager.update_metadata(step_6_file_name, 'step_6_start_time', str(datetime.now()))
        cleaned_data_file_name = 'cleaned_data.csv' # <-- name of the base file from files_with_raw_data_links.json
        #cleaned_df.to_csv(os.path.join(PROCESSED_DATA_DIR, 'cleaned_data.csv'), index=False)
        #logger.info(f"Cleaned data saved to {os.path.join(PROCESSED_DATA_DIR, 'cleaned_data.csv')}")
        # cleaned_df.to_excel(os.path.join(PROCESSED_DATA_DIR, 'cleaned_data.xlsx'), index=False, engine='openpyxl')
        cleaned_df.to_parquet(os.path.join(PROCESSED_DATA_DIR, 'cleaned_data.parquet'), index=False)
        logger.info(f"Cleaned data saved to {os.path.join(PROCESSED_DATA_DIR, 'cleaned_data.parquet')}")
        logger.info("Step 6: Save cleaned data completed successfully.")
        metadata_manager.update_metadata(step_6_file_name, 'step_6_status', 'completed')
        metadata_manager.update_metadata(step_6_file_name, 'step_6_end_time', str(datetime.now()))
        proceed_to_next_step(6)
        log_manager.log_info("Step 6: Cleaned data saved successfully.")

        # Step 7: Filter data - NOT IMPLEMENTED
        step_7_file_name = "7-raw file_name"
        logger.info("Continue data pipeline. Step 7 - NOT IMPLEMENTED: Filtering data...")
        metadata_manager.update_metadata(step_7_file_name, 'step', '7')
        metadata_manager.update_metadata(step_7_file_name, 'step_name', 'Filter data')
        metadata_manager.update_metadata(step_7_file_name, 'step_7_start_time', str(datetime.now()))
        proceed_to_next_step(7)
        log_manager.log_info("Step 7: Data filtering step not implemented.")

        log_manager.log_info("Data pipeline completed successfully.")
        metadata_manager.update_metadata(step_7_file_name, 'pipeline_status', 'completed')

    except Exception as e:
        log_manager.log_error(f"An error occurred: {e}")
        metadata_manager.update_metadata(0, 'pipeline_status', f'error: {e}')
        metadata_manager.update_metadata(0, 'error_time', str(datetime.now()))

if __name__ == "__main__":
    main()
