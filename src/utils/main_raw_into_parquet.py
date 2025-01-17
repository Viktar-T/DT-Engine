import os
import csv
import pandas as pd
import logging
import chardet
from src.config import RAW_DATA_DIR

def detect_encoding(file_path):
    with open(file_path, 'rb') as f:
        result = chardet.detect(f.read(100000))  # Read first 100KB
        return result['encoding']

# Function to determine the delimiter
def detect_delimiter(file_path, sample_size=1024):
    encoding = detect_encoding(file_path)
    with open(file_path, 'r', encoding=encoding) as file:
        sample = file.read(sample_size)  # Read a sample of the file
        sniffer = csv.Sniffer()
        delimiter = sniffer.sniff(sample).delimiter
    return delimiter

def transform_csv_to_parquet():
    source_dir = os.path.join(RAW_DATA_DIR, 'all_csv')
    target_dir = os.path.join(RAW_DATA_DIR, 'parquet_files')

    if not os.path.exists(target_dir):
        os.makedirs(target_dir)

    for root, _, files in os.walk(source_dir):
        for file in files:
            if file.endswith('.csv'):
                csv_file_path = os.path.join(root, file)
                
                detected_delimiter = detect_delimiter(csv_file_path)
                print(f"File: {file}. Detected delimiter: {detected_delimiter}")

                # Detect encoding
                encoding = detect_encoding(csv_file_path)
                #df = pd.read_csv(csv_file_path, sep=None, engine='python', encoding=encoding)
                #df = pd.read_csv(csv_file_path, sep='[;,]', engine='python', encoding=encoding)
                df = pd.read_csv(csv_file_path, delimiter=';', encoding=encoding)
                #df = pd.read_csv(csv_file_path, delimiter=detect_delimiter, encoding=encoding)

                # Log file name and DataFrame columns
                #logging.info(f"Processing file: {csv_file_path}")
                #logging.info(f"Columns in DataFrame: {df.columns.tolist()}")

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
    # For Python 3.9 and above
    logging.basicConfig(level=logging.INFO, encoding='utf-8', format='%(levelname)s: %(message)s')

    # For Python versions earlier than 3.9, use the custom handler method
    # import sys
    # handler = logging.StreamHandler(sys.stdout)
    # handler.setLevel(logging.INFO)
    # handler.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
    # handler.encoding = 'utf-8'
    # logger = logging.getLogger()
    # logger.addHandler(handler)
    # logger.setLevel(logging.INFO)

    transform_csv_to_parquet()
