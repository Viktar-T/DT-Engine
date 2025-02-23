import os

# Project Root Directory
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Directories
DATA_DIR = os.path.join(BASE_DIR, 'data')
RAW_DATA_DIR = os.getenv('RAW_DATA_DIR', os.path.join(DATA_DIR, 'raw'))
PROCESSED_DATA_DIR = os.getenv('PROCESSED_DATA_DIR', os.path.join(DATA_DIR, 'processed'))
PROCESSED_DATA_SEPARATE_FILES_DIR = os.path.join(PROCESSED_DATA_DIR, 'a_main_columns_only_separate_files')
PROCESSED_DATA_WITH_FUELS_FILE_DIR = os.path.join(PROCESSED_DATA_DIR, 'b_with_fuels_separate_files')
FUELS_DATA_DIR = os.path.join(DATA_DIR, 'fuels')
MODELS_DIR = os.path.join(DATA_DIR, 'models')
METADATA_DIR = os.path.join(DATA_DIR, 'metadata')
LOGS_DIR = os.path.join(BASE_DIR, 'logs')
NOTEBOOKS_DIR = os.path.join(BASE_DIR, 'notebooks')
#PARQUET_DATA_DIR = os.path.join(RAW_DATA_DIR, 'parquet_files')
RAW_PARQUET_DATA_DIR = os.path.join(RAW_DATA_DIR, 'parquet_files')
RAUGH_DATA_DIR = os.path.join(RAW_DATA_DIR, 'raugh')
RAUGH_CSV_DATA_DIR = os.path.join(RAUGH_DATA_DIR, 'csv_raugh')
RAUGH_XLSX_DATA_DIR = os.path.join(RAUGH_DATA_DIR, 'xlsx_raugh')
RAUGH_PARQUT_DATA_DIR = os.path.join(RAUGH_DATA_DIR, 'parquet_raugh')

# Model Metadata
MODEL_METADATA_DIR = os.path.join(MODELS_DIR, 'metadata')
PRETRAINED_MODELS_DIR = os.path.join(MODELS_DIR, 'pretrained')
VISUALIZATIONS_DIR = os.path.join(MODELS_DIR, 'visualizations')

# File Paths
HYPERPARAMETERS_FILE = os.path.join(MODEL_METADATA_DIR, 'hyperparameters.yaml')
MODEL_CONFIG_FILE = os.path.join(MODEL_METADATA_DIR, 'model_config.json')
MODEL_METRICS_FILE = os.path.join(MODEL_METADATA_DIR, 'model_v1_metrics.json')

# Logging Configuration
APP_LOG_FILE = os.path.join(LOGS_DIR, 'app.log')
TRAINING_LOG_FILE = os.path.join(LOGS_DIR, 'training.log')
LOGGING_LEVEL = 'INFO'  # Options: DEBUG, INFO, WARNING, ERROR, CRITICAL

# Data Processing Parameters
DEFAULT_MISSING_VALUE_STRATEGY = 'mean'  # Options: mean, median, drop
OUTLIER_THRESHOLD = 3.0  # Z-score threshold for outlier detection

# Neural Network Configuration
DEFAULT_BATCH_SIZE = 32
DEFAULT_LEARNING_RATE = 0.001
DEFAULT_EPOCHS = 50
TRAIN_TEST_SPLIT_RATIO = 0.8  # Train-test split

# Visualization Parameters
DEFAULT_FIGURE_SIZE = (10, 6)
SAVE_VISUALIZATIONS = True

# Utility Functions
def ensure_directories_exist():
    """
    Ensure that all required directories exist.
    """
    required_dirs = [
        DATA_DIR,
        RAW_DATA_DIR,
        PROCESSED_DATA_DIR,
        MODELS_DIR,
        MODEL_METADATA_DIR,
        PRETRAINED_MODELS_DIR,
        VISUALIZATIONS_DIR,
        LOGS_DIR,
        METADATA_DIR,
        RAW_PARQUET_DATA_DIR,
        RAUGH_DATA_DIR,
        RAUGH_CSV_DATA_DIR,
        RAUGH_XLSX_DATA_DIR,
        RAUGH_PARQUT_DATA_DIR,
    ]
    for directory in required_dirs:
        if not os.path.exists(directory):
            os.makedirs(directory)

# Call the directory check at import time
ensure_directories_exist()
