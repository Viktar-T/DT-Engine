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
4. load, clean, filter, transform all .parquet files with only_chosen_fuels.json. Run "main_all_files.py":
"C:\Users\vtaustyka\PycharmProjects\DT-Engine\main_all_files.py"
5. 

next step BUILDING MODELS
1. 