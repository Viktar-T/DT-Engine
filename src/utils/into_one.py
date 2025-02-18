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

required_columns_for_validation_step = [
  "Time",
  "Ciś. pow. za turb.[Pa]",
  "ECT - wyjście z sil.[°C]",
  "MAF[kg/h]",
  "Moc[kW]",
  "Moment obrotowy[Nm]",
  "Obroty[obr/min]",
  "Temp. oleju w misce[°C]",
  "Temp. pal. na wyjściu sil.[°C]",
  "Temp. powietrza za turb.[°C]",
  "Zużycie paliwa średnie[g/s]",
  "Zużycie paliwa bieżące[g/s]",
  "Temp. spalin mean[°C]",
  "Cetane number",
  "Density at 15 Â°C, kg/m3",
  "Viscosity at 40 Â°C, mm2/s",
  "Flash point, Â°C",
  "LHV (Lower Heating Value), MJ/kg"
]

# Read each file into a DataFrame with specified columns and store in a list
dfs = []
for f in parquet_files:
    try:
        df = pd.read_parquet(
            f,
            columns=required_columns_for_validation_step,
            engine='fastparquet'
        )
        dfs.append(df)
    except KeyError:
        print(f"File {f} does not contain all required columns. Skipping.")

# Concatenate all DataFrames
combined_df = pd.concat(dfs, ignore_index=True)

# Save the combined DataFrame to a new parquet file
combined_df.to_parquet(os.path.join(PROCESSED_DATA_WITH_FUELS_FILE_DIR, 'combined.parquet'))

# Print the shape of the resulting DataFrame
print(combined_df.shape)
