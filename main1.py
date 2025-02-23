import logging
import os
import pandas as pd
from src.data_loader import DataLoader
from src.data_validator import DataValidator
from src.data_cleaner import DataCleaner
from src.config import RAW_DATA_DIR, PROCESSED_DATA_DIR
from src.utils.build_json_with_files import JSONBuilder


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
        # Step 1: Build files_with_raw_data_links.json
        logger.info("Starting data pipeline...")
        logger.info("Step 1: Building files_with_raw_data_links.json...")
        builder = JSONBuilder(RAW_DATA_DIR)
        builder.build_json()
        output_file_path = os.path.join(RAW_DATA_DIR, 'files_with_raw_data_links.json')
        builder.save_json(output_file_path)
        logger.info("Step 1: files_with_raw_data_links.json built successfully.")
        proceed_to_next_step(1)

        # Step 2: Load raw data
        logger.info("Continue data pipeline. Step 2: Loading raw data...")
        data_loader = DataLoader(RAW_DATA_DIR)
        raw_data_frames = data_loader.select_from_json_and_load_data(selected_id=1)
        proceed_to_next_step(2)

        # Step 3: Validate data
        logger.info("Continue data pipeline. Step 3: Validating data...")
        required_columns_list = [required_columns_for_validation_step, required_columns_eco]
        validator = DataValidator(raw_data_frames, required_columns_list=required_columns_list)
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
        logger.info("Step 3: Data validated successfully.")
        proceed_to_next_step(3)

        # Step 4: Extract metadata (optional)
        logger.info("Continue data pipeline. Step 4: Extracting metadata...")
        metadata_list = validator.get_metadata()
        for idx, metadata in enumerate(metadata_list):
            logger.info(f"Metadata for DataFrame {idx}: {metadata}")
        proceed_to_next_step(4)

        #---> !!!# Step 5: Clean and preprocess data
        logger.info("Continue data pipeline. Step 5: Cleaning and preprocessing data...")
        data_cleaner = DataCleaner(df=raw_data_frames[0], required_columns=required_columns)
        cleaned_df = data_cleaner.clean()
        logger.info("Step 5: Data cleaned and preprocessed successfully.")
        proceed_to_next_step(5)

        # --> Step 6: Save cleaned data
        logger.info("Step 6: Continue data pipeline. Saving cleaned data...")
        cleaned_data_file_name = 'cleaned_data.csv' # <-- name of the base file from files_with_raw_data_links.json
        #cleaned_df.to_csv(os.path.join(PROCESSED_DATA_DIR, 'cleaned_data.csv'), index=False)
        #logger.info(f"Cleaned data saved to {os.path.join(PROCESSED_DATA_DIR, 'cleaned_data.csv')}")
        # cleaned_df.to_excel(os.path.join(PROCESSED_DATA_DIR, 'cleaned_data.xlsx'), index=False, engine='openpyxl')
        cleaned_df.to_parquet(os.path.join(PROCESSED_DATA_DIR, 'cleaned_data.parquet'), index=False)
        logger.info(f"Cleaned data saved to {os.path.join(PROCESSED_DATA_DIR, 'cleaned_data.parquet')}")
        logger.info("Step 6: Save cleaned data completed successfully.")
        proceed_to_next_step(6)

        logger.info("Data pipeline completed successfully.")

    except Exception as e:
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
