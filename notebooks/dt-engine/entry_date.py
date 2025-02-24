column_names_from_pl_to_en_full = {
    'Ciś. pow. za turb.[Pa]': ['Air Pressure After Turbo [Pa]', 'Turbo Pressure'],
    'ECT - wyjście z sil.[°C]': ['Engine Coolant Temperature at Engine Outlet [°C]', 'Coolant Temp'],
    'MAF[kg/h]': ['Mass Air Flow [kg/h]', 'MAF'],
    'Moc[kW]': ['Engine Power [kW]', 'Power'],
    'Moment obrotowy[Nm]': ['Engine Torque [Nm]', 'Torque'],
    'Obroty[obr/min]': ['Engine Speed [rpm]', 'RPM'],
    'Temp. oleju w misce[°C]': ['Oil Temperature in Sump [°C]', 'Oil Temp'],
    'Temp. pal. na wyjściu sil.[°C]': ['Fuel Temperature at Engine Outlet [°C]', 'Fuel Temp'],
    'Temp. powietrza za turb.[°C]': ['Air Temperature After Turbo [°C]', 'Turbo Air Temp'],
    'Temp. spalin mean[°C]': ['Exhaust Gas Temperature 1/6 [°C]', 'Exhaust Temp'],
    'Zużycie paliwa średnie[g/s]': ['Average Fuel Consumption [g/s]', 'Fuel Consump'],
    "Cetane number": ['Cetane Number', 'Cetane number'],
    "Density at 15 °C, kg/m3": ['Density at 15 °C', 'Density-15'],
    "Viscosity at 40 °C, mm2/s": ['Viscosity at 40 °C', 'Viscosity-40'],
    "Flash point, °C": ['Flash Point', 'Flash pt'],
    "LHV (Lower Heating Value), MJ/kg": ['LHV (Lower Heating Value)', 'LHV']
}

all_columns_name = [
    'Time', 'Turbo Pressure', 'Coolant Temp', 'MAF', 'Power', 'Torque', 'RPM', 
    'Oil Temp', 'Fuel Temp', 'Turbo Air Temp', 'Fuel Consump', 'Exhaust Temp', 
    'Cetane number', 'Density-15', 'Viscosity-40', 'Flash pt', 'LHV']

features = {
    "speed": [
        'Torque', 'RPM',      # 'Power' - dont use as it complex: 'Torque' and 'RPM'
        'Fuel Consump', 
        'Turbo Pressure', 'MAF',
        
        'Coolant Temp', 'Oil Temp', 'Fuel Temp', 'Turbo Air Temp',
        'Cetane number', 'Density-15', 'Viscosity-40', 'Flash pt', 'LHV'  # fuel properties 

        'Exhaust Temp',
    ],
    "load": [

        'Cetane number', 'Density-15', 'Viscosity-40', 'Flash pt', 'LHV'  # fuel properties
    ],
}   

targets = {
    "speed": [

    ],
    "load": [],
}