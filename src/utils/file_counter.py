import os
from src.config import RAW_DATA_DIR

def count_files_and_folders(directory):
    total_dirs = 0
    total_files = 0
    for entry in os.scandir(directory):
        if entry.is_dir():
            dirs, files = count_files_and_folders(entry.path)
            total_dirs += 1 + dirs
            total_files += files
        elif entry.is_file():
            total_files += 1
    print(f"Directory: {directory}, {total_dirs} directories, {total_files} files")
    return total_dirs, total_files


if __name__ == '__main__':
    diretcory_all_csv = os.path.join(RAW_DATA_DIR, 'all_csv')
    count_files_and_folders(diretcory_all_csv)
    print("------------------------------")
    diretcory_parquet_files = os.path.join(RAW_DATA_DIR, 'parquet_files')
    count_files_and_folders(diretcory_parquet_files)

