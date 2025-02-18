import pandas as pd
import glob
import os
from src.config import PROCESSED_DATA_WITH_FUELS_FILE_DIR

# Use the provided config directory instead of the script's directory
data_dir = PROCESSED_DATA_WITH_FUELS_FILE_DIR

# Get list of all .parquet files in the data_dir
parquet_files = glob.glob(os.path.join(data_dir, '*.parquet'))

# Check if any parquet files were found
if not parquet_files:
    print("No parquet files found in the specified directory.")
    exit()

# Updated columns list according to the new names provided
columns_in_dfs = [
    "Time",
    "Turbo Pressure",
    "Coolant Temp",
    "MAF",
    "Power",
    "Torque",
    "RPM",
    "Oil Temp",
    "Fuel Temp",
    "Turbo Air Temp",
    "Fuel Consump",
    "Exhaust Temp",
    "Cetane number",
    "Density-15",
    "Viscosity-40",
    "Flash pt",
    "LHV"
]

# Read each file into a DataFrame with specified columns and store in a list
dfs = []
for f in parquet_files:
    try:
        df = pd.read_parquet(
            f,
            columns=columns_in_dfs,
            engine='fastparquet'
        )
        dfs.append(df)
        print(df.shape)
    except KeyError:
        print(f"File {f} does not contain all required columns. Skipping.")

# Concatenate all DataFrames
combined_df = pd.concat(dfs, ignore_index=True)

# Save the combined DataFrame to a new parquet file
combined_df.to_parquet(os.path.join(PROCESSED_DATA_WITH_FUELS_FILE_DIR, 'combined.parquet'))

# Print the shape of the resulting DataFrame
print(combined_df.shape)
