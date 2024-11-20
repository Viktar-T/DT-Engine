import logging
from src.data_loader import DataLoader
from src.data_validator import DataValidator
from src.data_cleaner import DataCleaner
from src.config import RAW_DATA_DIR, PROCESSED_DATA_DIR

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def main():
    try:
        # Step 1: Load raw data
        logger.info("Starting data pipeline...")
        data_loader = DataLoader(RAW_DATA_DIR)
        raw_data = data_loader.load_data("Zew ch ON _ 2018-12-18.xlsx")
        logger.info(f"Data loaded successfully. Shape: {raw_data.shape}")

        # Step 2: Validate data
        required_columns = ['Ciś. pow. za turb.[Pa]', 'ECT - wyjście z sil.[°C]', 'MAF[kg/h]', 'Moc[kW]', 
                      'Moment obrotowy[Nm]', 'Obroty[obr/min]', 'Temp. oleju w misce[°C]', 
                      'Temp. pal. na wyjściu sil.[°C]', 'Temp. powietrza za turb.[°C]', 
                    'Temp. spalin 1/6[°C]', 'Temp. spalin 2/6[°C]', 'Temp. spalin 3/6[°C]', 'Temp. spalin 4/6[°C]',  # count average
                      'Zużycie paliwa średnie[g/s]', 
                    'Ciśnienie atmosferyczne[hPa]', 'Temp. otoczenia[°C]', 'Wilgotność względna[%]']  # Replace with your required columns
        optional_columns = ["Parameter3"]  # Replace with optional columns
        validator = DataValidator(raw_data, parameters_all=required_columns, optional_params=optional_columns)
        validation_results = validator.validate_columns()
        if not validation_results["valid"]:
            raise ValueError("Validation failed. Missing required columns.")

        # Step 3: Extract metadata (optional)
        metadata = validator.get_metadata()
        logger.info(f"Metadata:\n{metadata}")

        # Step 4: Clean and preprocess data
        cleaner = DataCleaner(raw_data, time_column="Time")
        harmonized_data = cleaner.harmonize_time(parameter_columns=["Parameter1", "Parameter2"])
        cleaned_data = cleaner.handle_missing_values(strategy="mean")

        # Step 5: Save cleaned data
        cleaned_data.to_csv(PROCESSED_DATA_DIR / "cleaned_data.csv", index=False)
        logger.info(f"Cleaned data saved to {PROCESSED_DATA_DIR / 'cleaned_data.csv'}")

    except Exception as e:
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
