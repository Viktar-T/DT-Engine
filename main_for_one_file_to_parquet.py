#import logging
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

# !!! NOT USED !!!
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

#current_file = {
#            "id": 4,        # <--- change for windows id:1; linux id:4
#            "main_file_name": "1600Nn obc ON _ 2018-12-06_ORIGEN.csv",
#            "eco_file_name": "",
#            "description": "empty",
#            "diesel_test_type": "1600Nn obc",
#            "fuel": "ON",
#            "diesel_engine_name": "empty",
#            "test_date": "2018-12-06"
#        }

def get_current_file_by_id(file_path, target_id):
    with open(file_path, 'r') as file:
        data = json.load(file)
    for item in data["Lublin Diesel"]:
        if item.get('id') == target_id:
            # Return the entire dictionary
            return item
    return None
id = 1
current_file = get_current_file_by_id(os.path.join(RAW_DATA_DIR, 'files_with_raw_data_links.json'), id)


names_of_files_under_procession = [current_file["main_file_name"], current_file["eco_file_name"], current_file["fuel"]]
files_for_steps = f"main_file_name:{names_of_files_under_procession[0]}, eco_file_name:{names_of_files_under_procession[1]}, Fuel:{names_of_files_under_procession[2]}"

required_columns_eco = ["OBR", "Mo", "CO", "HC", "LAMBDA", "CO2", "O2", "NO", "PM"]

# Configure logging


def proceed_to_next_step(step_number, log_manager):
    response = input(f"Step {step_number} was completed. Press [y/n] to proceed: ")
    if response.lower() != 'y':
        log_manager.log_info(f"Process stopped at step {step_number}.")
        exit()

def main():
    try:
        PARQUET_DATA_DIR = os.path.join(RAW_DATA_DIR, 'parquet_files')
        CSV_DATA_DIR = os.path.join(RAW_DATA_DIR, 'CSV_files')
        # Initialize LogManager
        log_manager = LogManager(logs_dir=LOGS_DIR, 
                                 names_of_files_under_procession=names_of_files_under_procession)
        log_manager.log_info("Starting data pipeline...")

        # Step 1: Build files_with_raw_data_links.json
        metadata_manager = MetadataManager(metadata_dir=METADATA_DIR, 
                                           names_of_files_under_procession=names_of_files_under_procession)
        metadata_manager.update_metadata('1-Build files_with_raw_data_links.json', 'pipeline_status', 'started')
        metadata_manager.update_metadata('1-Build files_with_raw_data_links.json', 'step', '1')
        metadata_manager.update_metadata('1-Build files_with_raw_data_links.json', 'step_name', 'Build files_with_raw_data_links.json')
        metadata_manager.update_metadata('1-Build files_with_raw_data_links.json', 'start_time', str(datetime.now()))

        log_manager.log_info("Step 1: Building files_with_raw_data_links.json...")
        builder = JSONBuilder(PARQUET_DATA_DIR)
        builder.build_json()
        output_file_path = os.path.join(PARQUET_DATA_DIR, 'files_with_raw_data_links.json')
        builder.save_json(output_file_path)
        log_manager.log_info("Step 1: files_with_raw_data_links.json built successfully.")
        metadata_manager.update_metadata("1-Build files_with_raw_data_links.json", 'step_1_status', 'completed')
        metadata_manager.update_metadata("1-Build files_with_raw_data_links.json", 'step_1_end_time', str(datetime.now()))
        proceed_to_next_step(1, log_manager)
        log_manager.log_info("Step 1: files_with_raw_data_links.json built successfully.")

        # Step 2: Load raw data
        step_2_file_name = f"2-{files_for_steps}"
        log_manager.log_info("Continue data pipeline. Step 2: Loading raw data...")
        metadata_manager.update_metadata(step_2_file_name, 'step', '2')
        metadata_manager.update_metadata(step_2_file_name, 'step_name', 'Load raw data')
        metadata_manager.update_metadata(step_2_file_name, 'step_2_start_time', str(datetime.now()))
        data_loader = DataLoader(PARQUET_DATA_DIR, 
                                 names_of_files_under_procession=names_of_files_under_procession,
                                 metadata_manager=metadata_manager, 
                                 log_manager=log_manager)
        raw_data_frames = data_loader.select_from_json_and_load_data(selected_id=current_file["id"])
        metadata_manager.update_metadata(step_2_file_name, 'step_2_status', 'completed')
        metadata_manager.update_metadata(step_2_file_name, 'step_2_end_time', str(datetime.now()))
        proceed_to_next_step(2, log_manager)
        log_manager.log_info("Step 2: Raw data loaded successfully.")

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
        proceed_to_next_step(3, log_manager)
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

        
        #data_visualizer = DataVisualizer(raw_data_frames[0]) # OK
        #columns_to_plot = required_columns_for_validation_step
        ##columns_to_plot = ['Zużycie paliwa średnie[g/s]']
        #data_visualizer.plot_columns(columns_to_plot)

        #filtered_df = data_filter.filter_columns()
        data_filter.filter_columns()

        #data_visualizer = DataVisualizer(filtered_df)
        #log_manager.log_info("!!!!!!!!!!!!!-1-ERRO search!!!!!!!!!!!!!")
        #columns_to_plot = required_columns_for_validation_step
        ##columns_to_plot = ['Zużycie paliwa średnie[g/s]']
        #data_visualizer.plot_columns(columns_to_plot)  #NOT OK

        #filtered_df = data_filter.synchronize_time()
        data_filter.synchronize_time()

        #data_visualizer = DataVisualizer(filtered_df)
        #log_manager.log_info("!!!!!!!!!!!!!-2-ERRO search!!!!!!!!!!!!!!!!!!!!")
        #columns_to_plot = required_columns_for_validation_step
        ##columns_to_plot = ['Zużycie paliwa średnie[g/s]']
        #data_visualizer.plot_columns(columns_to_plot)  #NOT OK

        filtered_df = data_filter.filter_all_stable_periods()
        
        # Proceed without re-initializing DataCleaner

        validator.get_metadata([filtered_df], message_for_logs="DataFrame after filtering and cleaning:")
        metadata_manager.update_metadata(step_5_file_name, 'step_5_status', 'completed')
        metadata_manager.update_metadata(step_5_file_name, 'step_5_end_time', str(datetime.now()))
        proceed_to_next_step(5, log_manager)
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
        proceed_to_next_step(6, log_manager)
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

        # Step 8.1: Visualize data 1
        step_8_file_name = f"8-{files_for_steps}"
        log_manager.log_info("Continue data pipeline. Step 8: Visualizing data...")
        metadata_manager.update_metadata(step_8_file_name, 'step', '8')
        metadata_manager.update_metadata(step_8_file_name, 'step_name', 'Visualize data')
        metadata_manager.update_metadata(step_8_file_name, 'step_8_start_time', str(datetime.now()))
        
        data_visualizer = DataVisualizer(filtered_df)
        columns_to_plot = ['Obroty[obr/min]', 'Moment obrotowy[Nm]', 'Moc[kW]', 'Zużycie paliwa średnie[g/s]']
        #columns_to_plot = required_columns_for_validation_step
        data_visualizer.plot_columns(columns_to_plot)

        #for column_pair in required_columns:
        #    x_column = column_pair[0]
        #    y_column = column_pair[1]
        #    data_visualizer.plot_parameter_vs_parameter(x_column, y_column)

       #x_column = 'Time'
       ##y_columns = ['Obroty[obr/min]', 'Moment obrotowy[Nm]', 'Moc[kW]', 'Zużycie paliwa średnie[g/s]']
       #y_columns = required_columns_for_validation_step
       #data_visualizer.plot_parameter_vs_parameters(x_column, y_columns)

        # x_column = 'Obroty[obr/min]'
        # y_columns = ['Moment obrotowy[Nm]', 'Moc[kW]', 'MAF[kg/h]']
        # data_visualizer.plot_parameter_vs_parameters(x_column, y_columns)
        
        # ...call other visualization methods as needed...

        metadata_manager.update_metadata(step_8_file_name, 'step_7_status', 'completed')
        metadata_manager.update_metadata(step_8_file_name, 'step_7_end_time', str(datetime.now()))
        proceed_to_next_step(7, log_manager)
        log_manager.log_info("Step 8: Data visualization completed successfully.")

        log_manager.log_info("Data pipeline completed successfully.")
        metadata_manager.update_metadata(step_8_file_name, 'pipeline_status', 'completed')

    except Exception as e:
        log_manager.log_error(f"An error occurred: {e}")
        metadata_manager.update_metadata(0, 'pipeline_status', f'error: {e}')
        metadata_manager.update_metadata(0, 'error_time', str(datetime.now()))

if __name__ == "__main__":
    main()
