import os
import json
import logging
from datetime import datetime
from typing import Any, Dict
import pandas as pd

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MetadataManager:
    def __init__(self, metadata_dir: str):
        self.metadata_dir = metadata_dir
        self.version = self._get_next_version()
        self.metadata = self._load_existing_metadata()

    def _get_next_version(self) -> str:
        existing_files = [f for f in os.listdir(self.metadata_dir) if f.startswith('metadata_v')]
        if not existing_files:
            return 'v1.0'
        latest_version = sorted(existing_files)[-1].split('_v')[-1].split('.json')[0]
        major, minor = map(int, latest_version.split('.'))
        return f'v{major}.{minor + 1}'

    def _load_existing_metadata(self) -> Dict[str, Any]:
        file_path = os.path.join(self.metadata_dir, f'metadata_{self.version}.json')
        if os.path.exists(file_path):
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {}

    def update_metadata(self, key: str, value: Any):
        if isinstance(value, pd.DataFrame):
            value = value.astype(str).to_dict(orient='list')
        elif isinstance(value, pd.Series):
            value = value.astype(str).to_dict()
        self.metadata[key] = value
        # logger.info(f"Updated metadata: {self.metadata}")
        self._save_metadata()

    def _save_metadata(self):
        file_path = os.path.join(self.metadata_dir, f'metadata_{self.version}.json')
        logger.info(f"!!!!!!!Saving metadata to {file_path}")
        
        # Load existing metadata if the file exists
        if os.path.exists(file_path):
            with open(file_path, 'r', encoding='utf-8') as f:
                existing_metadata = json.load(f)
        else:
            existing_metadata = {}
        
        logger.info(f"!!!Existing metadata: {existing_metadata}")
        logger.info(f"!!!New metadata to update: {self.metadata}")
        
        # Update existing metadata with new information
        existing_metadata.update(self.metadata)

        # Save the updated metadata back to the file
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(existing_metadata, f, ensure_ascii=False, indent=4)
        
        logger.info(f"Metadata saved to {file_path}")

    def visualize_metadata(self):
        # Implement visualization logic here
        pass

    def monitor_metadata(self):
        # Implement monitoring logic here
        pass
