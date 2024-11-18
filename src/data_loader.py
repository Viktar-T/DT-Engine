import pandas as pd
import os

def load_data(file_path: str) -> pd.DataFrame:
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File {file_path} not found.")
    return pd.read_csv(file_path)