import os
import json
import pandas as pd
import logging
from datetime import datetime
from typing import Any, Dict, Optional
from src.config import METADATA_DIR

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MetadataManager:
    """
    A class to handle dynamic metadata management, version control, and visualization.
    """

    def __init__(self, metadata_dir: str = METADATA_DIR):
        """
        Initialize the MetadataManager.

        Parameters:
        - metadata_dir (str): Directory where metadata files will be stored.
        """
        self.metadata_dir = metadata_dir
        self.metadata = {}
        self.current_version = 1.0
        self.metadata_file = self._get_new_metadata_file_path()

        # Ensure metadata directory exists
        if not os.path.exists(self.metadata_dir):
            os.makedirs(self.metadata_dir)
            logger.info(f"Created metadata directory: {self.metadata_dir}")

        logger.info("MetadataManager initialized.")

    def _get_new_metadata_file_path(self) -> str:
        """
        Generate a new metadata file path with versioning.

        Returns:
        - str: The path to the new metadata file.
        """
        while True:
            file_name = f"metadata_v{self.current_version:.1f}.json"
            file_path = os.path.join(self.metadata_dir, file_name)
            if not os.path.exists(file_path):
                return file_path
            self.current_version += 0.1

    def update_metadata(self, key: str, value: Any):
        """
        Update the metadata dictionary with a new key-value pair.

        Parameters:
        - key (str): The key to update.
        - value (Any): The value to associate with the key.
        """
        self.metadata[key] = value
        logger.info(f"Updated metadata: {key} = {value}")

    def save_metadata(self):
        """
        Save the current metadata to the JSON file.
        """
        with open(self.metadata_file, 'w', encoding='utf-8') as f:
            json.dump(self.metadata, f, indent=4, ensure_ascii=False)
        logger.info(f"Metadata saved to: {self.metadata_file}")

    def visualize_metadata(self):
        """
        Visualize the metadata in the log.
        """
        logger.info("Current Metadata:")
        for key, value in self.metadata.items():
            logger.info(f"{key}: {value}")

    def extract_metadata_from_dataframe(self, df: Optional[pd.DataFrame], name: str):
        """
        Extract metadata from a DataFrame and add it to the metadata.

        Parameters:
        - df (pd.DataFrame): The DataFrame to extract metadata from.
        - name (str): Name of the DataFrame or its purpose in the pipeline.
        """
        if df is not None:
            metadata = {
                "name": name,
                "rows": df.shape[0],
                "columns": df.shape[1],
                "size_in_memory": df.memory_usage(deep=True).sum(),
                "columns_info": df.dtypes.apply(str).to_dict(),
                "created_at": datetime.now().isoformat(),
            }
            self.metadata[name] = metadata
            logger.info(f"Extracted metadata for DataFrame '{name}': {metadata}")
        else:
            logger.warning(f"No metadata extracted: DataFrame '{name}' is None.")

    def increment_version(self):
        """
        Increment the metadata version and generate a new metadata file path.
        """
        self.current_version += 0.1
        self.metadata_file = self._get_new_metadata_file_path()
        logger.info(f"Metadata version incremented to {self.current_version:.1f}")

