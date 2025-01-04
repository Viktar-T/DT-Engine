import os
import pandas as pd
from datetime import datetime
from src.data_loader import DataLoader
from src.data_validator import DataValidator
from src.data_cleaner import DataCleaner
from src.config import (
    RAW_DATA_DIR, 
    PROCESSED_DATA_DIR, 
    PROCESSED_DATA_SEPARATE_FILES_DIR,
    PROCESSED_DATA_WITH_FUELS_FILE_DIR,
    METADATA_DIR, 
    LOGS_DIR, 
    RAW_PARQUET_DATA_DIR, 
    RAUGH_CSV_DATA_DIR, 
    RAUGH_XLSX_DATA_DIR, 
    RAUGH_PARQUT_DATA_DIR
)
from src.metadata_manager import MetadataManager
from src.log_manager import LogManager
from src.data_visualizer import DataVisualizer
from src.data_transformation import DataTransformation
from src.data_filter import DataFilter
import json

required_columns_for_validation_step = [
    'Ciś. pow. za turb.[Pa]', 
    'Ciśnienie atmosferyczne[hPa]', 
    'ECT - wyjście z sil.[°C]', 
    'MAF[kg/h]', 
    'Moc[kW]', 
    'Moment obrotowy[Nm]', 
    'Obroty[obr/min]', 
    'Temp. oleju w misce[°C]', 
    'Temp. otoczenia[°C]',  
    'Temp. pal. na wyjściu sil.[°C]', 
    'Temp. powietrza za turb.[°C]',
    'Temp. spalin 1/6[°C]', 
    'Temp. spalin 2/6[°C]', 
    'Temp. spalin 3/6[°C]', 
    'Temp. spalin 4/6[°C]', 
    'Wilgotność względna[%]', 
    'Zużycie paliwa średnie[g/s]'
]

required_columns_eco = ["OBR", "Mo", "CO", "HC", "LAMBDA", "CO2", "O2", "NO", "PM"]

# json_path = os.path.join(RAW_PARQUET_DATA_DIR, 'files_with_raw_data_links.json')
json_path = os.path.join(RAW_PARQUET_DATA_DIR, 'only_chosen_fuels.json')
with open(json_path, 'r') as f:
    json_data_links = json.load(f)


def process_file(item: dict, metadata_manager: MetadataManager, log_manager: LogManager) -> None:
    """Process a single item (file) through the entire data pipeline."""
    json_item_id = item['id']
    main_file_name = item['main_file_name']
    eco_file_name = item['eco_file_name']
    fuel_name = item['fuel']

    input_file_path = os.path.join(RAW_PARQUET_DATA_DIR, main_file_name)
    
    if not os.path.exists(input_file_path):
        log_manager.log_error(f"File with ID:{json_item_id} not found: {input_file_path}")
        return
    
    log_manager.log_info(f"Processing file with ID:{json_item_id}: {main_file_name}")

    names_of_files_under_procession = [main_file_name, eco_file_name, fuel_name]
    files_for_steps = f"main_file_name:{main_file_name}, eco_file_name:{eco_file_name}, Fuel:{fuel_name}"

    try:
        # Step 2: Load raw data
        step_2_file_name = f"Step 2. Item with ID:{json_item_id}. Files: {files_for_steps}"
        log_manager.log_info("Step 2: Loading raw data...")
        metadata_manager.update_metadata(step_2_file_name, 'step', '2')
        metadata_manager.update_metadata(step_2_file_name, 'step_name', 'Load raw data')
        metadata_manager.update_metadata(step_2_file_name, 'step_2_start_time', str(datetime.now()))

        data_loader = DataLoader(
            RAW_PARQUET_DATA_DIR, 
            names_of_files_under_procession=names_of_files_under_procession,
            metadata_manager=metadata_manager, 
            log_manager=log_manager
        )
        raw_data_frames = data_loader.select_from_json_and_load_data(selected_id=json_item_id)

        metadata_manager.update_metadata(step_2_file_name, 'step_2_status', 'completed')
        metadata_manager.update_metadata(step_2_file_name, 'step_2_end_time', str(datetime.now()))
        log_manager.log_info("Step 2: Raw data loaded successfully.")

        # Step 3: Validate data
        step_3_file_name = f"Step 3. Item with ID:{json_item_id}. Files: {files_for_steps}"
        log_manager.log_info("Step 3: Validating data...")
        metadata_manager.update_metadata(step_3_file_name, 'step', '3')
        metadata_manager.update_metadata(step_3_file_name, 'step_name', 'Validate data')
        metadata_manager.update_metadata(step_3_file_name, 'step_3_start_time', str(datetime.now()))

        # don't use "required_columns_eco"
        required_columns_list = [required_columns_for_validation_step, required_columns_eco]
        validator = DataValidator(
            raw_data_frames, 
            required_columns_list=required_columns_list, 
            file_names=[main_file_name, eco_file_name],
            names_of_files_under_procession=names_of_files_under_procession,
            log_manager=log_manager,
            metadata_manager=metadata_manager
        )
        validation_results = validator.validate_columns()
        for idx, result in enumerate(validation_results):
            if not result["valid"]:
                # Handle missing columns for each DataFrame
                validator.handle_missing_columns(fill_value=0)
                log_manager.log_info(f"DataFrame {idx}: Missing required columns filled with default values.")

        # Schema validation
        expected_schemas = [
            {col: 'numeric' for col in required_columns_for_validation_step},
            {col: 'numeric' for col in required_columns_eco}
        ]
        validator.validate_schema(expected_schemas)
        validator.check_for_duplicate_columns()
        reports = validator.generate_report()
        for report in reports:
            log_manager.log_info(report)

        log_manager.log_info("Step 3: Data validated successfully.")
        metadata_manager.update_metadata(step_3_file_name, 'validation_results', validation_results)
        metadata_manager.update_metadata(step_3_file_name, 'step_3_status', 'completed')
        metadata_manager.update_metadata(step_3_file_name, 'step_3_end_time', str(datetime.now()))

        # Step 4: Extract metadata
        step_4_file_name = f"Step 4. Item with ID:{json_item_id}. Files: {files_for_steps}"
        metadata_list = validator.get_metadata()
        metadata_manager.update_metadata(step_4_file_name, 'step_4_status', 'completed')
        metadata_manager.update_metadata(step_4_file_name, 'step_4_end_time', str(datetime.now()))
        log_manager.log_info("Step 4: Metadata extracted successfully.")

        # Step 5: Filter and preprocess data
        step_5_file_name = f"Step 5. Item with ID:{json_item_id}. Files: {files_for_steps}"
        log_manager.log_info("Step 5: Filtering and preprocessing data...")
        metadata_manager.update_metadata(step_5_file_name, 'step', '5')
        metadata_manager.update_metadata(step_5_file_name, 'step_name', 'Filter and preprocess data')
        metadata_manager.update_metadata(step_5_file_name, 'step_5_start_time', str(datetime.now()))

        data_filter = DataFilter(
            df=raw_data_frames[0],
            required_columns=required_columns_for_validation_step,
            names_of_files_under_procession=names_of_files_under_procession,
            metadata_manager=metadata_manager,
            log_manager=log_manager
        )

        data_filter.filter_columns()
        data_filter.synchronize_time()
        filtered_df = data_filter.filter_all_stable_periods()

        validator.get_metadata([filtered_df], message_for_logs="DataFrame after filtering:")
        metadata_manager.update_metadata(step_5_file_name, 'step_5_status', 'completed')
        metadata_manager.update_metadata(step_5_file_name, 'step_5_end_time', str(datetime.now()))
        log_manager.log_info("Step 5: Data filtered and preprocessed successfully.")

        
        # Step 7: Transform data
        step_6_file_name = f"Step 6. Item with ID:{json_item_id}. Files: {files_for_steps}"
        log_manager.log_info("Step 6: Transforming data...")
        data_transformation = DataTransformation(
            df=filtered_df,
            names_of_files_under_procession=names_of_files_under_procession,
            log_manager=log_manager,
            metadata_manager=metadata_manager
        )

        corrected_df = data_transformation.atmospheric_power_correction(show_corrections=True)
        corrected_df = data_transformation.exhaust_gas_mean_temperature_calculation()

        #Step 8: Add fuel data
        

        # Save transformed data
        #transformed_data_parquet_path = os.path.join(PROCESSED_DATA_SEPARATE_FILES_DIR, f'transformed_data_{main_file_name}.parquet')
        transformed_data_parquet_path = os.path.join(PROCESSED_DATA_WITH_FUELS_FILE_DIR, f'{main_file_name}_tr_f.parquet')
        corrected_df.to_parquet(transformed_data_parquet_path, index=False)

        metadata_manager.update_metadata(step_6_file_name, 'step_7_status', 'completed')
        metadata_manager.update_metadata(step_6_file_name, 'step_7_end_time', str(datetime.now()))
        log_manager.log_info("Step 7: Data transformation completed successfully.")

        # Pipeline completed for this file
        log_manager.log_info("Data pipeline completed successfully.")
        metadata_manager.update_metadata(step_6_file_name, 'pipeline_status', 'completed')

    except Exception as e:
        log_manager.log_error(f"An error occurred: {e}")
        metadata_manager.update_metadata("pipeline_error", 'pipeline_status', f'error: {e}')
        metadata_manager.update_metadata("pipeline_error", 'error_time', str(datetime.now()))


def main():
    # Initialize LogManager and MetadataManager once (assuming they can be reused)
    log_manager = LogManager(
        logs_dir=LOGS_DIR, 
        names_of_files_under_procession=[] # Will be updated dynamically in process_file
    )
    metadata_manager = MetadataManager(
        metadata_dir=METADATA_DIR, 
        names_of_files_under_procession=[]
    )

    # Process each file in the JSON
    for item_from_main_json in json_data_links.get('Lublin Diesel', []):
        process_file(item_from_main_json, metadata_manager, log_manager)


if __name__ == "__main__":
    main()
