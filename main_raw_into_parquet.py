import os
import pandas as pd
from src.config import RAW_DATA_DIR

def transform_csv_to_parquet():
    source_dir = os.path.join(RAW_DATA_DIR, 'csv_files')
    target_dir = os.path.join(RAW_DATA_DIR, 'parquet_files')

    if not os.path.exists(target_dir):
        os.makedirs(target_dir)

    for root, _, files in os.walk(source_dir):
        for file in files:
            if file.endswith('.csv'):
                csv_file_path = os.path.join(root, file)
                df = pd.read_csv(csv_file_path, delimiter=';', encoding='cp1250')

                # Deduplicate column names
                def deduplicate_columns(columns):
                    counts = {}
                    new_columns = []
                    for col in columns:
                        if col in counts:
                            counts[col] += 1
                            new_columns.append(f"{col}_{counts[col]}")
                        else:
                            counts[col] = 0
                            new_columns.append(col)
                    return new_columns

                df.columns = deduplicate_columns(df.columns)

                # Extract folder name (e.g., '2014-11')
                folder_name = os.path.basename(os.path.dirname(csv_file_path))

                # Construct output file name
                base_filename = os.path.splitext(file)[0]
                output_filename = f"{base_filename} - {folder_name}.parquet"
                output_file_path = os.path.join(target_dir, output_filename)

                # Save DataFrame as .parquet file
                df.to_parquet(output_file_path)

if __name__ == '__main__':
    transform_csv_to_parquet()
