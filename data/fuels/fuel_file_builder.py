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
    "Test Method": [
        "EN ISO 5165", "EN ISO 4264", "EN ISO 3675/EN ISO 12185", "EN ISO 3104",
        "EN ISO 20846/20884", "EN ISO 2719", "EN ISO 10370", "EN ISO 6245",
        "EN ISO 12937", "EN ISO 12662", "EN 14078", "EN ISO 12916",
        "EN ISO 2160", "EN ISO 12156-1", "EN 15751", "EN ISO 3405",
        "EN ISO 3405", "EN ISO 3405", "-", "-", "EN 16576", None
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
    
    #'ON_Diesel_fuel_for_dt': [None] * rows_num,
    
    # Removed the following columns:
    # "BIOW_for_dt": [None] * rows_num,                   #pure methyl esters of used frying oils
    # "BIOW50_for_dt": [None] * rows_num,                 #50 % of pure methyl esters of used frying oils
    # "U75_for_dt": [None] * rows_num,                    #mixture of methyl esters of waste vegetable oils 75% with diesel oil – 25% volume fraction
    # "HVO_for_dt": [None] * rows_num,                    #hydrogenated vegetable oil
    # "HVO25_for_dt": [None] * rows_num,                  #hydrogenated vegetable oil 25% volume fraction
    # "Efecta_Agrotronika_for_dt": [None] * rows_num,     # "EFECTA Diesel" improved diesel fuel with the trade name Efecta Agrotronika
    # "B20_for_dt": [None] * rows_num,                    #mixture of diesel oil with rapeseed oil methyl esters 20% volume fraction
    # "HHO_for_dt": [None] * rows_num,                    #??hydrogenated heating oil
    #"AG2": [None] * rows_num,                           # NOT use for training Models mixture of diesel oil with the addition of nanosilver (2%) dissolved in water

}



# Add 'Verwa_from_ORLEN Lower Limit' and 'Verwa_from_ORLEN Upper Limit' to fuel_data
fuel_data['Verwa_from_ORLEN Lower Limit'] = [
    51.0,         # Cetane number
    46.0,         # Cetane index
    815.0,        # Density at 15°C
    2.000,        # Viscosity at 40°C
    None,         # Sulphur content
    56.0,         # Flash point
    None,         # Carbon residue
    None,         # Ash content
    None,         # Water content
    None,         # Total contamination
    None,         # FAME content
    None,         # Polycyclic aromatic hydrocarbons
    None,         # Copper strip corrosion
    None,         # Lubricity
    20.0,         # Oxidation Stability
    None,         # Distillation recovered at 250°C
    5.0,          # Distillation recovered at 350°C
    None,         # 95% recovered at
    None,         # Cold Filter Plugging Point (winter)
    None,         # Cold Filter Plugging Point (summer)
    None,         # Manganese content
    None          # Short description
]

fuel_data['Verwa_from_ORLEN Upper Limit'] = [
    None,         # Cetane number
    None,         # Cetane index
    845.0,        # Density at 15°C
    4.500,        # Viscosity at 40°C
    None,         # Sulphur content
    None,         # Flash point
    0.30,         # Carbon residue
    0.010,        # Ash content
    200.0,        # Water content
    24.0,         # Total contamination
    7.0,          # FAME content
    None,         # Polycyclic aromatic hydrocarbons
    None,         # Copper strip corrosion
    460.0,        # Lubricity
    None,         # Oxidation Stability
    65.0,         # Distillation recovered at 250°C
    None,         # Distillation recovered at 350°C
    360.0,        # 95% recovered at
    -10.0,        # Cold Filter Plugging Point (winter)
    0.0,          # Cold Filter Plugging Point (summer)
    None,         # Manganese content
    None          # Short description
]

# Add 'HVO Lower Limit' and 'HVO Upper Limit' to fuel_data
fuel_data['HVO Lower Limit'] = [
    80.0,         # Cetane number
    None,         # Cetane index
    770.0,        # Density at 15°C
    2.5,          # Viscosity at 40°C
    None,         # Sulphur content
    61.0,         # Flash point
    None,         # Carbon residue
    None,         # Ash content
    None,         # Water content
    None,         # Total contamination
    0.0,          # FAME content
    0.0,          # Polycyclic aromatic hydrocarbons
    'Class 1',    # Copper strip corrosion
    None,         # Lubricity
    None,         # Oxidation Stability
    None,         # Distillation recovered at 250°C
    None,         # Distillation recovered at 350°C
    None,         # 95% recovered at
    -34.0,        # Cold Filter Plugging Point (winter)
    None,         # Cold Filter Plugging Point (summer)
    None,         # Manganese content
    None          # Short description
]

fuel_data['HVO Upper Limit'] = [
    99.0,         # Cetane number
    None,         # Cetane index
    790.0,        # Density at 15°C
    3.5,          # Viscosity at 40°C
    5.0,          # Sulphur content
    None,         # Flash point
    0.10,         # Carbon residue
    0.001,        # Ash content
    200.0,        # Water content
    10.0,         # Total contamination
    0.0,          # FAME content
    0.0,          # Polycyclic aromatic hydrocarbons
    'Class 1',    # Copper strip corrosion
    None,         # Lubricity
    None,         # Oxidation Stability
    None,         # Distillation recovered at 250°C
    360.0,        # Distillation recovered at 350°C
    320.0,        # 95% recovered at
    None,         # Cold Filter Plugging Point (winter)
    -5.0,         # Cold Filter Plugging Point (summer)
    None,         # Manganese content
    None          # Short description
]

# Add 'HVO25 Lower Limit' and 'HVO25 Upper Limit' to fuel_data
fuel_data['HVO25 Lower Limit'] = [
    60.0,         # Cetane number
    None,         # Cetane index
    800.0,        # Density at 15°C
    2.8,          # Viscosity at 40°C
    None,         # Sulphur content
    55.0,         # Flash point
    None,         # Carbon residue
    None,         # Ash content
    None,         # Water content
    None,         # Total contamination
    0.0,          # FAME content
    None,         # Polycyclic aromatic hydrocarbons
    'Class 1',    # Copper strip corrosion
    None,         # Lubricity
    None,         # Oxidation Stability
    35.0,         # Distillation recovered at 250°C
    90.0,         # Distillation recovered at 350°C
    350.0,        # 95% recovered at
    -15.0,        # Cold Filter Plugging Point (winter)
    -5.0,         # Cold Filter Plugging Point (summer)
    None,         # Manganese content
    None          # Short description
]

fuel_data['HVO25 Upper Limit'] = [
    65.0,         # Cetane number
    None,         # Cetane index
    820.0,        # Density at 15°C
    3.5,          # Viscosity at 40°C
    10.0,         # Sulphur content
    None,         # Flash point
    0.15,         # Carbon residue
    0.005,        # Ash content
    200.0,        # Water content
    20.0,         # Total contamination
    0.0,          # FAME content
    None,         # Polycyclic aromatic hydrocarbons
    'Class 1',    # Copper strip corrosion
    None,         # Lubricity
    None,         # Oxidation Stability
    45.0,         # Distillation recovered at 250°C
    95.0,         # Distillation recovered at 350°C
    360.0,        # 95% recovered at
    -10.0,        # Cold Filter Plugging Point (winter)
    0.0,          # Cold Filter Plugging Point (summer)
    None,         # Manganese content
    None          # Short description
]

# Add 'BIOW Lower Limit' and 'BIOW Upper Limit' to fuel_data
fuel_data['BIOW Lower Limit'] = [
    45.0,         # Cetane number
    None,         # Cetane index
    870.0,        # Density at 15°C
    4.9,          # Viscosity at 40°C
    None,         # Sulphur content
    130.0,        # Flash point
    None,         # Carbon residue
    None,         # Ash content
    None,         # Water content
    None,         # Total contamination
    96.5,         # FAME content
    None,         # Polycyclic aromatic hydrocarbons
    'Pass',       # Copper strip corrosion
    None,         # Lubricity
    None,         # Oxidation Stability
    None,         # Distillation recovered at 250°C
    None,         # Distillation recovered at 350°C
    None,         # 95% recovered at
    None,         # Cold Filter Plugging Point (winter)
    None,         # Cold Filter Plugging Point (summer)
    None,         # Manganese content
    None          # Short description
]

fuel_data['BIOW Upper Limit'] = [
    51.0,         # Cetane number
    None,         # Cetane index
    885.0,        # Density at 15°C
    5.6,          # Viscosity at 40°C
    None,         # Sulphur content
    160.0,        # Flash point
    None,         # Carbon residue
    None,         # Ash content
    None,         # Water content
    None,         # Total contamination
    100.0,        # FAME content
    None,         # Polycyclic aromatic hydrocarbons
    'Pass',       # Copper strip corrosion
    None,         # Lubricity
    None,         # Oxidation Stability
    None,         # Distillation recovered at 250°C
    None,         # Distillation recovered at 350°C
    None,         # 95% recovered at
    None,         # Cold Filter Plugging Point (winter)
    None,         # Cold Filter Plugging Point (summer)
    None,         # Manganese content
    None          # Short description
]

# Add 'BIOW50 Lower Limit' and 'BIOW50 Upper Limit' to fuel_data
fuel_data['BIOW50 Lower Limit'] = [
    47.0,         # Cetane Number
    None,         # Cetane Index
    850.0,        # Density at 15°C
    3.5,          # Viscosity at 40°C
    None,         # Sulphur Content
    100.0,        # Flash Point
    None,         # Carbon Residue
    None,         # Ash Content
    None,         # Water Content
    None,         # Total Contamination
    40.0,         # Heating Value (LHV)
    None,         # Polycyclic Aromatic Hydrocarbons
    None,         # Copper Strip Corrosion
    None,         # Lubricity
    None,         # Oxidation Stability
    None,         # Distillation Recovered at 250°C
    None,         # Distillation Recovered at 350°C
    None,         # 95% Recovered at
    None,         # Cold Filter Plugging Point (winter)
    None,         # Cold Filter Plugging Point (summer)
    None,         # Manganese Content
    None          # Short Description
]

fuel_data['BIOW50 Upper Limit'] = [
    49.0,         # Cetane Number
    None,         # Cetane Index
    870.0,        # Density at 15°C
    4.5,          # Viscosity at 40°C
    None,         # Sulphur Content
    130.0,        # Flash Point
    None,         # Carbon Residue
    None,         # Ash Content
    None,         # Water Content
    None,         # Total Contamination
    42.0,         # Heating Value (LHV)
    None,         # Polycyclic Aromatic Hydrocarbons
    None,         # Copper Strip Corrosion
    None,         # Lubricity
    None,         # Oxidation Stability
    None,         # Distillation Recovered at 250°C
    None,         # Distillation Recovered at 350°C
    None,         # 95% Recovered at
    None,         # Cold Filter Plugging Point (winter)
    None,         # Cold Filter Plugging Point (summer)
    None,         # Manganese Content
    None          # Short Description
]

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

# Convert 'Verwa_from_ORLEN Lower Limit' to numeric
fuel_df['Verwa_from_ORLEN Lower Limit'] = pd.to_numeric(
    fuel_df['Verwa_from_ORLEN Lower Limit'], errors='coerce').fillna(0)

# Convert 'Verwa_from_ORLEN Upper Limit' to numeric
fuel_df['Verwa_from_ORLEN Upper Limit'] = pd.to_numeric(
    fuel_df['Verwa_from_ORLEN Upper Limit'], errors='coerce').fillna(0)

# Convert 'HVO Lower Limit' to numeric
fuel_df['HVO Lower Limit'] = pd.to_numeric(
    fuel_df['HVO Lower Limit'], errors='coerce').fillna(0)

# Convert 'HVO Upper Limit' to numeric
fuel_df['HVO Upper Limit'] = pd.to_numeric(
    fuel_df['HVO Upper Limit'], errors='coerce').fillna(0)

# Convert 'HVO25 Lower Limit' to numeric
fuel_df['HVO25 Lower Limit'] = pd.to_numeric(
    fuel_df['HVO25 Lower Limit'], errors='coerce').fillna(0)

# Convert 'HVO25 Upper Limit' to numeric
fuel_df['HVO25 Upper Limit'] = pd.to_numeric(
    fuel_df['HVO25 Upper Limit'], errors='coerce').fillna(0)

# Convert 'BIOW Lower Limit' to numeric
fuel_df['BIOW Lower Limit'] = pd.to_numeric(
    fuel_df['BIOW Lower Limit'], errors='coerce').fillna(0)

# Convert 'BIOW Upper Limit' to numeric
fuel_df['BIOW Upper Limit'] = pd.to_numeric(
    fuel_df['BIOW Upper Limit'], errors='coerce').fillna(0)

# Convert 'BIOW50 Lower Limit' to numeric
fuel_df['BIOW50 Lower Limit'] = pd.to_numeric(
    fuel_df['BIOW50 Lower Limit'], errors='coerce').fillna(0)

# Convert 'BIOW50 Upper Limit' to numeric
fuel_df['BIOW50 Upper Limit'] = pd.to_numeric(
    fuel_df['BIOW50 Upper Limit'], errors='coerce').fillna(0)

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


fuel_df['Verwa_for_dt'] = np.where(
    (fuel_df['Verwa_from_ORLEN Lower Limit'] != 0) & (fuel_df['Verwa_from_ORLEN Upper Limit'] != 0),
    (fuel_df['Verwa_from_ORLEN Lower Limit'] + fuel_df['Verwa_from_ORLEN Upper Limit']) / 2,
    np.where(
        fuel_df['Verwa_from_ORLEN Lower Limit'] != 0,
        fuel_df['Verwa_from_ORLEN Lower Limit'],
        fuel_df['Verwa_from_ORLEN Upper Limit']
    )
)

fuel_df['HVO_for_dt'] = np.where(
    (fuel_df['HVO Lower Limit'] != 0) & (fuel_df['HVO Upper Limit'] != 0),
    (fuel_df['HVO Lower Limit'] + fuel_df['HVO Upper Limit']) / 2,
    np.where(
        fuel_df['HVO Lower Limit'] != 0,
        fuel_df['HVO Lower Limit'],
        fuel_df['HVO Upper Limit']
    )
)

fuel_df['HVO25_for_dt'] = np.where(
    (fuel_df['HVO25 Lower Limit'] != 0) & (fuel_df['HVO25 Upper Limit'] != 0),
    (fuel_df['HVO25 Lower Limit'] + fuel_df['HVO25 Upper Limit']) / 2,
    np.where(
        fuel_df['HVO25 Lower Limit'] != 0,
        fuel_df['HVO25 Lower Limit'],
        fuel_df['HVO25 Upper Limit']
    )
)

fuel_df['BIOW_for_dt'] = np.where(
    (fuel_df['BIOW Lower Limit'] != 0) & (fuel_df['BIOW Upper Limit'] != 0),
    (fuel_df['BIOW Lower Limit'] + fuel_df['BIOW Upper Limit']) / 2,
    np.where(
        fuel_df['BIOW Lower Limit'] != 0,
        fuel_df['BIOW Lower Limit'],
        fuel_df['BIOW Upper Limit']
    )
)

fuel_df['BIOW50_for_dt'] = np.where(
    (fuel_df['BIOW50 Lower Limit'] != 0) & (fuel_df['BIOW50 Upper Limit'] != 0),
    (fuel_df['BIOW50 Lower Limit'] + fuel_df['BIOW50 Upper Limit']) / 2,
    np.where(
        fuel_df['BIOW50 Lower Limit'] != 0,
        fuel_df['BIOW50 Lower Limit'],
        fuel_df['BIOW50 Upper Limit']
    )
)

# Remove or comment out the following lines to prevent overwriting 'BIOW_for_dt'
# fuel_df["BIOW_for_dt"] = [None] * rows_num
# fuel_df.at[rows_num - 1, "BIOW_for_dt"] = "pure methyl esters of used frying oils"

# Comment out the following lines to prevent overwriting 'BIOW50_for_dt'
# fuel_df["BIOW50_for_dt"] = [None] * rows_num
# fuel_df.at[rows_num - 1, "BIOW50_for_dt"] = "50 % of pure methyl esters of used frying oils"

fuel_df["U75_for_dt"] = [None] * rows_num
fuel_df.at[rows_num - 1, "U75_for_dt"] = "mixture of methyl esters of waste vegetable oils 75% with diesel oil – 25% volume fraction"

# Comment out or remove the following lines:
# fuel_df["HVO_for_dt"] = [None] * rows_num
# fuel_df.at[rows_num - 1, "HVO_for_dt"] = "hydrogenated vegetable oil"

# Comment out or remove the following lines:
# fuel_df["HVO25_for_dt"] = [None] * rows_num
# fuel_df.at[rows_num - 1, "HVO25_for_dt"] = "hydrogenated vegetable oil 25% volume fraction"

fuel_df["Efecta_Agrotronika_for_dt"] = [None] * rows_num
fuel_df['Efecta_Agrotronika_for_dt'] = fuel_df['diesel_fuel_for_dt']
fuel_df.at[rows_num - 1, "Efecta_Agrotronika_for_dt"] = "??? improved diesel fuel with the trade name Efecta Agrotronika"

# Convert 'diesel_fuel_for_dt' to string and assign to 'Efecta_Agrotronika_for_dt'
fuel_df["Efecta_Agrotronika_for_dt"] = fuel_df["diesel_fuel_for_dt"].astype('string')

# Assign the descriptive string to the last row
fuel_df.at[rows_num - 1, "Efecta_Agrotronika_for_dt"] = "??? improved diesel fuel with the trade name Efecta Agrotronika"

fuel_df["B20_for_dt"] = [None] * rows_num
fuel_df.at[rows_num - 1, "B20_for_dt"] = "mixture of diesel oil with rapeseed oil methyl esters 20% volume fraction"

# fuel_df["HHO_for_dt"] = [None] * rows_num
# fuel_df.at[rows_num - 1, "HHO_for_dt"] = "??hydrogenated heating oil"



# Ensure the target directory exists
os.makedirs(FUELS_DATA_DIR, exist_ok=True)

# Outputting the DataFrame to various file formats
fuel_df.to_csv(os.path.join(FUELS_DATA_DIR, 'diesel_fuel_characteristics.csv'), index=False)
fuel_df.to_excel(os.path.join(FUELS_DATA_DIR, 'diesel_fuel_characteristics.xlsx'), index=False)
fuel_df.to_parquet(os.path.join(FUELS_DATA_DIR, 'diesel_fuel_characteristics.parquet'), index=False)
