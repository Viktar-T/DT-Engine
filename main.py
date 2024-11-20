import logging
import os
from src.data_loader import DataLoader
from src.data_validator import DataValidator
from src.data_cleaner import DataCleaner
from src.config import RAW_DATA_DIR, PROCESSED_DATA_DIR
from src.utils.build_json_with_files import JSONBuilder

required_columns = [
    'Ciś. pow. za turb.[Pa]', 'ECT - wyjście z sil.[°C]', 'MAF[kg/h]', 'Moc[kW]', 
    'Moment obrotowy[Nm]', 'Obroty[obr/min]', 'Temp. oleju w misce[°C]', 
    'Temp. pal. na wyjściu sil.[°C]', 'Temp. powietrza za turb.[°C]', 
    'Temp. spalin 1/6[°C]', 'Temp. spalin 2/6[°C]', 'Temp. spalin 3/6[°C]', 'Temp. spalin 4/6[°C]', 
    'Zużycie paliwa średnie[g/s]', 'Ciśnienie atmosferyczne[hPa]', 'Temp. otoczenia[°C]', 'Wilgotność względna[%]'
]


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
        logger.info("Building files_with_raw_data_links.json...")
        builder = JSONBuilder(RAW_DATA_DIR)
        builder.build_json()
        output_file_path = os.path.join(RAW_DATA_DIR, 'files_with_raw_data_links.json')
        builder.save_json(output_file_path)
        logger.info("files_with_raw_data_links.json built successfully.")
        proceed_to_next_step(1)

        # Step 2: Load raw data
        logger.info("Continue data pipeline. Loading raw data...")
        data_loader = DataLoader(RAW_DATA_DIR)
        #raw_data = data_loader.load_data("Zew ch ON _ 2018-12-18.xlsx")
        raw_data = data_loader.select_from_json_and_load_data(selected_id=1)
        logger.info(f"Data loaded successfully. Data Frame 1 - Shape: {raw_data[0].shape}") # !!!Think to multiple data frames
        proceed_to_next_step(2)

        # Step 3: Validate data
        logger.info("Continue data pipeline. Validating data...")
        validator = DataValidator(raw_data, parameters_all=required_columns)
        validation_results = validator.validate_columns()
        if not validation_results["valid"]:
            raise ValueError("Validation failed. Missing required columns.")
        logger.info("Data validated successfully.")
        proceed_to_next_step(3)

        # Step 4: Extract metadata (optional)
        metadata = validator.get_metadata()
        logger.info(f"Metadata:\n{metadata}")
        proceed_to_next_step(4)

        # Step 5: Clean and preprocess data
        logger.info("Continue data pipeline. Cleaning and preprocessing data...")
        cleaner = DataCleaner(raw_data, time_column="Time")
        harmonized_data = cleaner.harmonize_time(parameter_columns=["Parameter1", "Parameter2"])
        cleaned_data = cleaner.handle_missing_values(strategy="mean")
        logger.info("Data cleaned and preprocessed successfully.")
        proceed_to_next_step(5)

        # Step 6: Save cleaned data
        logger.info("Continue data pipeline. Saving cleaned data...")
        cleaned_data.to_csv(os.path.join(PROCESSED_DATA_DIR, "cleaned_data.csv"), index=False)
        logger.info(f"Cleaned data saved to {os.path.join(PROCESSED_DATA_DIR, 'cleaned_data.csv')}")
        proceed_to_next_step(6)

    except Exception as e:
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
