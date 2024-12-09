import os
import pandas as pd
import numpy as np
from src.config import FUELS_DATA_DIR

# Creating a dictionary of EN 590 diesel fuel characteristics
rows_num = 22
fuel_data = {
    "Property": [
        "Cetane number", "Cetane index", "Density at 15°C", "Viscosity at 40°C",
        "Sulphur content", "Flash point", "Carbon residue", "Ash content",
        "Water content", "Total contamination", "FAME content",
        "Polycyclic aromatic hydrocarbons", "Copper strip corrosion",
        "Lubricity", "Oxidation Stability", "Distillation recovered at 250°C",
        "Distillation recovered at 350°C", "95% recovered at",
        "Cold Filter Plugging Point (winter)", "Cold Filter Plugging Point (summer)",
        "Manganese content", "Short description"
    ],
    "Unit": [
        "-", "-", "kg/m³", "mm²/s", "mg/kg", "°C", "% m/m", "% m/m",
        "mg/kg", "mg/kg", "% v/v", "% m/m", "index", "µm", "h",
        "% v/v", "% v/v", "°C", "°C", "°C", "mg/l", None
    ],
    "EN590 Lower Limit": [
        51.0, 46.0, 820, 2.0, None, 55, None, None,
        None, None, None, None, "Class 1", None, 20,
        None, 85, None, None, None, None, None
    ],
    "EN590 Upper Limit": [
        None, None, 845, 4.5, 10, None, 0.30, 0.01,
        200, 24, 7, 8, "Class 1", 460, None,
        65, None, 360, -15, -5, 2.0, None
    ],
    "Test Method": [
        "EN ISO 5165", "EN ISO 4264", "EN ISO 3675/EN ISO 12185", "EN ISO 3104",
        "EN ISO 20846/20884", "EN ISO 2719", "EN ISO 10370", "EN ISO 6245",
        "EN ISO 12937", "EN ISO 12662", "EN 14078", "EN ISO 12916",
        "EN ISO 2160", "EN ISO 12156-1", "EN 15751", "EN ISO 3405",
        "EN ISO 3405", "EN ISO 3405", "-", "-", "EN 16576", None
    ], 
    'ON_Diesel_fuel_for_dt': [None] * rows_num,
    "Verwa_for_dt": [None] * rows_num,                  #improved diesel fuel with the trade name Verwa
    "BIOW_for_dt": [None] * rows_num,                   #pure methyl esters of used frying oils
    "BIOW50_for_dt": [None] * rows_num,                 #50 % of pure methyl esters of used frying oils
    "U75_for_dt": [None] * rows_num,                    #mixture of methyl esters of waste vegetable oils 75% with diesel oil – 25% volume fraction
    "HVO_for_dt": [None] * rows_num,                    #hydrogenated vegetable oil
    "HVO25_for_dt": [None] * rows_num,                  #hydrogenated vegetable oil 25% volume fraction
    "Efecta_Agrotronika_for_dt": [None] * rows_num,     #??? improved diesel fuel with the trade name Efecta Agrotronika
    "B20_for_dt": [None] * rows_num,                    #mixture of diesel oil with rapeseed oil methyl esters 20% volume fraction
    "HHO_for_dt": [None] * rows_num,                    #??hydrogenated heating oil
    "AG2": [None] * rows_num,                           # NOT use for training Models mixture of diesel oil with the addition of nanosilver (2%) dissolved in water

}

# Converting the dictionary into a DataFrame
fuel_df = pd.DataFrame(fuel_data)

# Step 1: Convert 'Lower Limit' to numeric, coercing errors
fuel_df['EN590 Lower Limit'] = pd.to_numeric(fuel_df['EN590 Lower Limit'], errors='coerce')
# Step 2: Handle NaN values in 'Lower Limit'
fuel_df['EN590 Lower Limit'] = fuel_df['EN590 Lower Limit'].fillna(0)

# Step 3: Convert 'Upper Limit' to numeric, coercing errors
fuel_df['EN590 Upper Limit'] = pd.to_numeric(fuel_df['EN590 Upper Limit'], errors='coerce')
# Step 4: Handle NaN values in 'Upper Limit'
fuel_df['EN590 Upper Limit'] = fuel_df['EN590 Upper Limit'].fillna(0)

# Add "diesel_fuel_for_dt" column based on "Lower Limit" and "Upper Limit"
fuel_df['diesel_fuel_for_dt'] = np.where(
    (fuel_df['EN590 Lower Limit'] != 0) & (fuel_df['EN590 Upper Limit'] != 0),
    (fuel_df['EN590 Lower Limit'] + fuel_df['EN590 Upper Limit']) / 2,
    np.where(
        fuel_df['EN590 Lower Limit'] != 0,
        fuel_df['EN590 Lower Limit'],
        fuel_df['EN590 Upper Limit']
    )
)

# Outputting the DataFrame to various file formats
fuel_df.to_csv(os.path.join(FUELS_DATA_DIR, 'diesel_fuel_characteristics.csv'), index=False)
fuel_df.to_excel(os.path.join(FUELS_DATA_DIR, 'diesel_fuel_characteristics.xlsx'), index=False)
fuel_df.to_parquet(os.path.join(FUELS_DATA_DIR, 'diesel_fuel_characteristics.parquet'), index=False)
