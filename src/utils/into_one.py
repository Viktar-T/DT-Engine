import pandas as pd
import glob
import os

# Get the directory of the script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Get list of all .parquet files in the script directory
parquet_files = glob.glob(os.path.join(script_dir, '*.parquet'))

# Check if any parquet files were found
if not parquet_files:
    print("No parquet files found in the current directory.")
    exit()

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
    'Temp. spalin mean[°C]', 
    'Wilgotność względna[%]', 
    'Zużycie paliwa średnie[g/s]'
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
combined_df.to_parquet('combined.parquet')

# Print the shape of the resulting DataFrame
print(combined_df.shape)
