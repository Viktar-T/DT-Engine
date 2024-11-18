import os
import pytest
from src import config

# Test constants
def test_base_directory():
    """
    Test that BASE_DIR points to the correct base directory.
    """
    expected_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    assert config.BASE_DIR == expected_path, "BASE_DIR is not correctly set."

def test_directory_paths():
    """
    Test that all defined directory paths in config exist.
    """
    directories = [
        config.DATA_DIR,
        config.RAW_DATA_DIR,
        config.PROCESSED_DATA_DIR,
        config.MODELS_DIR,
        config.MODEL_METADATA_DIR,
        config.PRETRAINED_MODELS_DIR,
        config.VISUALIZATIONS_DIR,
        config.LOGS_DIR,
    ]
    for directory in directories:
        assert os.path.exists(directory), f"Directory does not exist: {directory}"

def test_file_paths():
    """
    Test that all defined file paths in config are correctly set.
    """
    file_paths = [
        config.HYPERPARAMETERS_FILE,
        config.MODEL_CONFIG_FILE,
        config.MODEL_METRICS_FILE,
    ]
    for file_path in file_paths:
        assert isinstance(file_path, str), f"File path {file_path} is not a string."
        assert os.path.join(config.MODEL_METADATA_DIR, os.path.basename(file_path)) == file_path

def test_logging_configuration():
    """
    Test that logging configurations are correctly set.
    """
    assert config.APP_LOG_FILE.endswith("app.log"), "APP_LOG_FILE is not correctly named."
    assert config.TRAINING_LOG_FILE.endswith("training.log"), "TRAINING_LOG_FILE is not correctly named."
    assert config.LOGGING_LEVEL in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], (
        "LOGGING_LEVEL is not valid."
    )

def test_data_processing_parameters():
    """
    Test that data processing parameters are correctly configured.
    """
    assert config.DEFAULT_MISSING_VALUE_STRATEGY in ["mean", "median", "drop"], (
        "DEFAULT_MISSING_VALUE_STRATEGY has an invalid value."
    )
    assert isinstance(config.OUTLIER_THRESHOLD, float), "OUTLIER_THRESHOLD must be a float."

def test_neural_network_configuration():
    """
    Test that neural network configuration parameters are correctly set.
    """
    assert isinstance(config.DEFAULT_BATCH_SIZE, int), "DEFAULT_BATCH_SIZE must be an integer."
    assert config.DEFAULT_BATCH_SIZE > 0, "DEFAULT_BATCH_SIZE must be greater than 0."
    assert isinstance(config.DEFAULT_LEARNING_RATE, float), "DEFAULT_LEARNING_RATE must be a float."
    assert config.DEFAULT_LEARNING_RATE > 0, "DEFAULT_LEARNING_RATE must be greater than 0."
    assert isinstance(config.DEFAULT_EPOCHS, int), "DEFAULT_EPOCHS must be an integer."
    assert config.DEFAULT_EPOCHS > 0, "DEFAULT_EPOCHS must be greater than 0."
    assert 0 < config.TRAIN_TEST_SPLIT_RATIO <= 1, "TRAIN_TEST_SPLIT_RATIO must be between 0 and 1."

def test_visualization_parameters():
    """
    Test that visualization parameters are correctly set.
    """
    assert isinstance(config.DEFAULT_FIGURE_SIZE, tuple), "DEFAULT_FIGURE_SIZE must be a tuple."
    assert len(config.DEFAULT_FIGURE_SIZE) == 2, "DEFAULT_FIGURE_SIZE must have two elements."
    assert all(isinstance(i, int) for i in config.DEFAULT_FIGURE_SIZE), (
        "DEFAULT_FIGURE_SIZE must contain integers."
    )
    assert isinstance(config.SAVE_VISUALIZATIONS, bool), "SAVE_VISUALIZATIONS must be a boolean."

@pytest.fixture
def temporary_directories(monkeypatch, tmp_path):
    """
    Fixture to test directory creation using temporary paths.
    """
    monkeypatch.setattr(config, "BASE_DIR", str(tmp_path))
    config.ensure_directories_exist()
    return tmp_path

def test_ensure_directories_exist(temporary_directories):
    """
    Test that the `ensure_directories_exist` function creates required directories.
    """
    required_dirs = [
        config.DATA_DIR,
        config.RAW_DATA_DIR,
        config.PROCESSED_DATA_DIR,
        config.MODELS_DIR,
        config.MODEL_METADATA_DIR,
        config.PRETRAINED_MODELS_DIR,
        config.VISUALIZATIONS_DIR,
        config.LOGS_DIR,
    ]
    for directory in required_dirs:
        assert os.path.exists(directory), f"Directory was not created: {directory}"