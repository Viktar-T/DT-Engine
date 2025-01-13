BUILD ENVIRONMENT
0. use one of:
environmentUbuntu.yml 
environmentUbuntu.yml
1. install miniconda or conda:
https://docs.conda.io/projects/conda/en/latest/user-guide/install/windows.html
'conda --version'
2. Run the following command to ensure the updated PATH is stored in the user-level environment variables:
[Environment]::GetEnvironmentVariable("Path", "User") -split ";"

3. Check if the following paths are listed:
C:\Users\taustykav\AppData\Local\miniconda3
C:\Users\taustykav\AppData\Local\miniconda3\Scripts
C:\Users\taustykav\AppData\Local\miniconda3\Library\bin

4. Add the Miniconda paths to the system PATH:
[Environment]::SetEnvironmentVariable("Path", $env:Path + ";C:\Users\taustykav\AppData\Local\miniconda3;C:\Users\taustykav\AppData\Local\miniconda3\Scripts;C:\Users\taustykav\AppData\Local\miniconda3\Library\bin", "Machine")

5. 'conda --version'

6. conda env create -f environmentWindows.yml

7. To activate this environment, use
#     $ conda activate DT-Engine-env
# To deactivate an active environment, use
#     $ conda deactivate

PROCESSING
1. we have in all_csv folders with "2018-12"
"C:\Users\vtaustyka\PycharmProjects\DT-Engine\data\raw\all_csv"
2. run 
"src\utils\main_raw_into_parquet.py"
take each file from "all_csv" and transform to .parquet and save to
data\raw\parquet_files
3. to build .json for ALL .parquet and for chosen_fuels .parquet files:
"data\raw\parquet_files\files_with_raw_data_links.json"
run:
"C:\Users\vtaustyka\PycharmProjects\DT-Engine\src\utils\build_json_with_files.py"
4. load, clean, filter, transform, add fuels properties. Under procession files .parquet files from only_chosen_fuels.json. Run "main_all_files.py":
"C:\Users\vtaustyka\PycharmProjects\DT-Engine\main_all_files.py"
5. 

next step BUILDING MODELS
1. 