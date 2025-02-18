import os
import pandas as pd
import json
import unicodedata
from src.config import PROCESSED_DATA_WITH_FUELS_FILE_DIR

def get_columns():
    df = pd.read_parquet(os.path.join(PROCESSED_DATA_WITH_FUELS_FILE_DIR, "1200 obc - 2015-05.parquet_tr_f.parquet"))
    # Normalize unicode characters to ensure they are fully readable
    columns = [unicodedata.normalize("NFC", col) for col in df.columns.tolist()]
    with open(os.path.join(PROCESSED_DATA_WITH_FUELS_FILE_DIR, "columns.json"), "w", encoding="utf-8") as f:
        json.dump(columns, f, indent=2, ensure_ascii=False)
    return columns

if __name__ == "__main__":
    get_columns()
