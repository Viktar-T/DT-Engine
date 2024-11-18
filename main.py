from src.data_loader import DataLoader

def main():
    # Initialize the DataLoader
    data_loader = DataLoader()

    # List all CSV and Excel files in the raw data directory
    files = data_loader.list_files()
    print("Available data files:")
    for file in files:
        print(file)

    # Load data from the first file in the list (if any)
    if files:
        file_to_load = files[0]
        print(f"\nLoading data from: {file_to_load}")
        data = data_loader.load_data(file_to_load)
        print(f"Data loaded successfully. Here are the first few rows:\n{data.head()}")
    else:
        print("No data files found in the raw data directory.")

if __name__ == "__main__":
    main()


"""
from ingestion.data_loader import DataLoader
from preprocessing.data_cleaner import DataCleaner
from transformation.feature_engineering import FeatureEngineer
from visualization.data_visualizer import DataVisualizer
from storage.data_saver import DataSaver

def main_pipeline():
    # Ingestion
    data = DataLoader().load_all_data()

    # Preprocessing
    cleaned_data = DataCleaner().clean_data(data)

    # Transformation
    transformed_data = FeatureEngineer().apply_transformations(cleaned_data)

    # Visualization
    DataVisualizer().generate_plots(transformed_data)

    # Storage
    DataSaver().save_to_parquet(transformed_data, "final_data.parquet")

if __name__ == "__main__":
    main_pipeline()
"""