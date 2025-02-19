# DT-Engine Setup & Processing Instructions

This document outlines the steps to build the environment, process data, and the next steps for building models.

---

## Build Environment

1. **Choose Environment File**
   - Use one of:
     - `environmentUbuntu.yml`
     - `environmentWindows.yml`

2. **Install Miniconda/Conda**
   - Follow the installation instructions:  
     [Conda Installation for Windows](https://docs.conda.io/projects/conda/en/latest/user-guide/install/windows.html)
   - Verify the installation:
     ```bash
     conda --version
     ```

3. **Ensure Updated PATH is Stored**
   - Run the following PowerShell command to check the user-level PATH:
     ```powershell
     [Environment]::GetEnvironmentVariable("Path", "User") -split ";"
     ```

4. **Verify Miniconda Paths**
   - Ensure that the following paths are listed:
     - `C:\Users\taustykav\AppData\Local\miniconda3`
     - `C:\Users\taustykav\AppData\Local\miniconda3\Scripts`
     - `C:\Users\taustykav\AppData\Local\miniconda3\Library\bin`

5. **Add Miniconda Paths to System PATH**
   - Run the following command to add the paths to the system-level PATH:
     ```powershell
     [Environment]::SetEnvironmentVariable("Path", $env:Path + ";C:\Users\taustykav\AppData\Local\miniconda3;C:\Users\taustykav\AppData\Local\miniconda3\Scripts;C:\Users\taustykav\AppData\Local\miniconda3\Library\bin", "Machine")
     ```

6. **Verify Conda Installation Again**
   - Check the conda version:
     ```bash
     conda --version
     ```

7. **Create the Conda Environment**
   - Run the following command:
     ```bash
     conda env create -f environmentWindows.yml
     ```

8. **Activate/Deactivate the Environment**
   - To **activate** the environment:
     ```bash
     conda activate DT-Engine-env
     ```
   - To **deactivate** the environment:
     ```bash
     conda deactivate
     ```

---

## Processing

1. **Raw CSV Files**
   - All CSV files (e.g., files containing "2018-12") are located in:
     ```
     C:\Users\vtaustyka\PycharmProjects\DT-Engine\data\raw\all_csv
     ```

2. **Convert CSV Files to Parquet**
   - Run the script to convert each CSV file in the `all_csv` folder to a Parquet file:
     ```bash
     src\utils\main_raw_into_parquet.py
     ```
   - The converted files will be saved to:
     ```
     data\raw\parquet_files
     ```

3. **Build JSON with File Links**
   - Generate a JSON file that links to all Parquet files (including chosen fuels) by running:
     ```bash
     C:\Users\vtaustyka\PycharmProjects\DT-Engine\src\utils\build_json_with_files.py
     ```
   - The JSON file will be created at:
     ```
     data\raw\parquet_files\files_with_raw_data_links.json
     ```

4. **Process and Transform Data**
   - Load, clean, filter, transform the data, and add fuel properties for the Parquet files specified in `only_chosen_fuels.json`.
   - Execute the main processing script:
     ```bash
     C:\Users\vtaustyka\PycharmProjects\DT-Engine\main_all_files.py
     ```

5. **Merge Data**
   - Run the merging script:
     ```bash
     src\utils\into_one.py
     ```

---

## Building Models

- Alanizi NAWA 
