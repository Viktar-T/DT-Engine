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