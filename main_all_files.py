import os
import pandas as pd
from datetime import datetime
from src.data_loader import DataLoader
from src.data_validator import DataValidator
from src.data_cleaner import DataCleaner
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

# Load the JSON file containing the list of files
json_path = os.path.join(RAW_PARQUET_DATA_DIR, 'files_with_raw_data_links.json')
with open(json_path, 'r') as f:
    data_links = json.load(f)

# Process each file in the list
for item in data_links['Lublin Diesel']:
    main_file_name = item['main_file_name']
    input_file_path = os.path.join(RAW_PARQUET_DATA_DIR, main_file_name)
    
    if not os.path.exists(input_file_path):
        print(f"File not found: {input_file_path}")
        continue
    
    print(f"Processing file: {main_file_name}")
    
    # Read the parquet file
    df = pd.read_parquet(input_file_path)
    
    try: 
        # Initialize LogManager
        log_manager = LogManager(logs_dir=LOGS_DIR, 
                                 names_of_files_under_procession=names_of_files_under_procession)
        log_manager.log_info("Starting data pipeline...")

        ## Step 1: Build files_with_raw_data_links.json
        metadata_manager = MetadataManager(metadata_dir=METADATA_DIR, 
                                           names_of_files_under_procession=names_of_files_under_procession)

        # Step 2: Load raw data
        step_2_file_name = f"2-{files_for_steps}"
        log_manager.log_info("Continue data pipeline. Step 2: Loading raw data...")
        metadata_manager.update_metadata(step_2_file_name, 'step', '2')
        metadata_manager.update_metadata(step_2_file_name, 'step_name', 'Load raw data')
        metadata_manager.update_metadata(step_2_file_name, 'step_2_start_time', str(datetime.now()))
        data_loader = DataLoader(DIR_FOR_PROC, 
                                 names_of_files_under_procession=names_of_files_under_procession,
                                 metadata_manager=metadata_manager, 
                                 log_manager=log_manager)
        raw_data_frames = data_loader.select_from_json_and_load_data(selected_id=current_file["id"])
        metadata_manager.update_metadata(step_2_file_name, 'step_2_status', 'completed')
        metadata_manager.update_metadata(step_2_file_name, 'step_2_end_time', str(datetime.now()))
        log_manager.log_info("Step 2: Raw data loaded successfully.")
        proceed_to_next_step(2, log_manager)        

        # Step 3: Validate data
        step_3_file_name = f"3-{files_for_steps}"
        log_manager.log_info("Continue data pipeline. Step 3: Validating data...")
        metadata_manager.update_metadata(step_3_file_name, 'step', '3')
        metadata_manager.update_metadata(step_3_file_name, 'step_name', 'Validate data')
        metadata_manager.update_metadata(step_3_file_name, 'step_3_start_time', str(datetime.now()))
        required_columns_list = [required_columns_for_validation_step, required_columns_eco]
        validator = DataValidator(raw_data_frames, required_columns_list=required_columns_list, 
                                  file_names=[current_file["main_file_name"], current_file["eco_file_name"]],
                                  names_of_files_under_procession=names_of_files_under_procession,
                                  log_manager=log_manager,
                                  metadata_manager=metadata_manager)
        validation_results = validator.validate_columns()
        for idx, result in enumerate(validation_results):
            if not result["valid"]:
                # Handle missing columns for each DataFrame
                validator.handle_missing_columns(fill_value=0)
                log_manager.log_info(f"DataFrame {idx}: Missing required columns filled with default values.")

        # Schema validation
        expected_schemas = [
            {col: 'numeric' for col in required_columns_for_validation_step},       # Expected schema for first DataFrame
            {col: 'numeric' for col in required_columns_eco}    # Expected schema for second DataFrame
        ]
        validator.validate_schema(expected_schemas)
        validator.check_for_duplicate_columns()
        reports = validator.generate_report()
        for report in reports:
            log_manager.log_info(report)
        # metadata_manager.update_metadata(step_3_file_name, 'validation_reports', reports)
        log_manager.log_info("Step 3: Data validated successfully.")
        metadata_manager.update_metadata(step_3_file_name, 'validation_results', validation_results)
        metadata_manager.update_metadata(step_3_file_name, 'step_3_status', 'completed')
        metadata_manager.update_metadata(step_3_file_name, 'step_3_end_time', str(datetime.now()))
        log_manager.log_info("Step 3: Data validated successfully.")

        # Step 4: Validate data
        step_4_file_name = f"4-{files_for_steps}"
        metadata_list = validator.get_metadata()
        #for idx, metadata in enumerate(metadata_list):
        #    log_manager.log_info(f"Metadata for DataFrame {idx}: {metadata}")
        # metadata_manager.update_metadata(step_4_file_name, 'metadata_list', )
        metadata_manager.update_metadata(step_4_file_name, 'step_4_status', 'completed')
        metadata_manager.update_metadata(step_4_file_name, 'step_4_end_time', str(datetime.now()))
        proceed_to_next_step(4, log_manager)
        log_manager.log_info("Step 4: Metadata extracted successfully.")

        # Step 5: Filter and then clean 
        step_5_file_name = f"5-{files_for_steps}"
        log_manager.log_info("Continue data pipeline. Step 5: Filtering and preprocessing data...")
        metadata_manager.update_metadata(step_5_file_name, 'step', '5')
        metadata_manager.update_metadata(step_5_file_name, 'step_name', 'Filter and preprocess data')
        metadata_manager.update_metadata(step_5_file_name, 'step_5_start_time', str(datetime.now()))

        # Use DataFilter to filter columns and synchronize time
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
        
        # Proceed without re-initializing DataCleaner

        validator.get_metadata([filtered_df], message_for_logs="DataFrame after filtering and cleaning:")
        metadata_manager.update_metadata(step_5_file_name, 'step_5_status', 'completed')
        metadata_manager.update_metadata(step_5_file_name, 'step_5_end_time', str(datetime.now()))
        log_manager.log_info("Step 5: Data filtered and preprocessed successfully.")

        # Step 6: Save cleaned data
        step_6_file_name = f"6-{files_for_steps}"
        log_manager.log_info("Step 6: Continue data pipeline. Saving cleaned data...")
        metadata_manager.update_metadata(step_6_file_name, 'step', '6')
        metadata_manager.update_metadata(step_6_file_name, 'step_name', 'Save filtered data')
        metadata_manager.update_metadata(step_6_file_name, 'step_6_start_time', str(datetime.now()))
        filtered_df.to_csv(os.path.join(PROCESSED_DATA_DIR, f'filtered_data_{names_of_files_under_procession[0]}'), index=False)
        log_manager.log_info(f"Filtered and Cleaned data saved to {os.path.join(PROCESSED_DATA_DIR, f'cleaned_data_{names_of_files_under_procession[0]}')}")
        # cleaned_df.to_excel(os.path.join(PROCESSED_DATA_DIR, f'cleaned_data_{names_of_files_under_procession[0]}.xlsx'), index=False, engine='openpyxl')
        filtered_df.to_parquet(os.path.join(PROCESSED_DATA_DIR, 
                                           f'cleaned_data_{names_of_files_under_procession[0]}'), 
                                           index=False)
        log_manager.log_info(f"Filtered and Cleaned data saved to {os.path.join(PROCESSED_DATA_DIR, 'filtered_data.parquet')}")
        log_manager.log_info("Step 6: Save cleaned data completed successfully.")
        metadata_manager.update_metadata(step_6_file_name, 'step_6_status', 'completed')
        metadata_manager.update_metadata(step_6_file_name, 'step_6_end_time', str(datetime.now()))
        log_manager.log_info("Step 6: Filtered and Cleaned data saved successfully.")


        # Step 7: Transform data
        data_transformation = DataTransformation(
           df=filtered_df,
           names_of_files_under_procession=names_of_files_under_procession,
           log_manager=log_manager,
           metadata_manager=metadata_manager
           )

        # Apply atmospheric power correction and show corrections in logs
        corrected_df = data_transformation.atmospheric_power_correction(show_corrections=True)
        corrected_df = data_transformation.exhaust_gas_mean_temperature_calculation()


        metadata_manager.update_metadata(step_8_file_name, 'step_7_status', 'completed')
        metadata_manager.update_metadata(step_8_file_name, 'step_7_end_time', str(datetime.now()))
        log_manager.log_info("Step 8: Data visualization completed successfully.")

        log_manager.log_info("Data pipeline completed successfully.")
        metadata_manager.update_metadata(step_8_file_name, 'pipeline_status', 'completed')

    except Exception as e:
        log_manager.log_error(f"An error occurred: {e}")
        metadata_manager.update_metadata(0, 'pipeline_status', f'error: {e}')
        metadata_manager.update_metadata(0, 'error_time', str(datetime.now()))
    
    # Save the processed data
    output_file_path = os.path.join(PROCESSED_DATA_DIR, main_file_name)
    df.to_parquet(output_file_path)
