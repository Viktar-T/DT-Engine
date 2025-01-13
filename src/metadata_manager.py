import os
import json
import logging
from datetime import datetime
from typing import Any, Dict, List
import pandas as pd

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MetadataManager:
    def __init__(self, metadata_dir: str, 
                 names_of_files_under_procession: List[str] = None):
        
        self.metadata_dir = metadata_dir
        self.names_of_files_under_procession = names_of_files_under_procession
        self.version = self._get_next_version()
        self.metadata = self._load_existing_metadata()
        if not os.path.exists(self.metadata_dir):
            os.makedirs(self.metadata_dir)

    def _get_next_version(self) -> str:
        existing_files = [f for f in os.listdir(self.metadata_dir) if f.startswith('metadata_v')]
        if not existing_files:
            return 'v1.0'
        
        versions = [f.split('_v')[-1].split('.json')[0] for f in existing_files]
        versions = sorted(versions, key=lambda s: list(map(int, s.split('.'))))
        latest_version = versions[-1]
        major, minor = map(int, latest_version.split('.'))
        return f'v{major}.{minor + 1}'

    def _load_existing_metadata(self) -> Dict[str, Any]:
        file_path = os.path.join(self.metadata_dir, f'metadata_{self.version}.json')
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except json.JSONDecodeError:
                logger.error(f"Corrupted JSON file at {file_path}. Initializing empty metadata.")
                return {}
            except FileNotFoundError:
                logger.info(f"Metadata file {file_path} not found. Initializing empty metadata.")
                return {}
            except Exception as e:
                logger.error(f"Unexpected error loading metadata: {e}")
                return {}
        return {}

    def update_metadata(self, step: int, key: str, value: Any):
        if isinstance(value, pd.DataFrame):
            value = value.astype(str).to_dict(orient='list')
        elif isinstance(value, pd.Series):
            value = value.astype(str).to_dict()
        if str(step) not in self.metadata:
            self.metadata[str(step)] = {}
        self.metadata[str(step)][key] = value
        self._save_metadata()

    def _save_metadata(self):
        file_path = os.path.join(self.metadata_dir, f'metadata_{self.version}.json')
        # Load existing metadata if the file exists
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                existing_metadata = json.load(f)
        except json.JSONDecodeError:
            logger.error(f"Corrupted JSON file at {file_path}. Overwriting with new metadata.")
            existing_metadata = {}
        except FileNotFoundError:
            logger.info(f"Metadata file {file_path} not found. Creating a new one.")
            existing_metadata = {}
        except Exception as e:
            logger.error(f"Unexpected error loading existing metadata: {e}")
            existing_metadata = {}
        
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
    
    # not used in main_all_files.py te same method in data_validator.py
    def get_metadata(self) -> List[pd.DataFrame]:
        """
        Extract metadata for each DataFrame.
        
        Returns:
        - A list of DataFrames with column metadata for each DataFrame.
        """
        logger.info("Starting metadata extraction for multiple DataFrames...")
        metadata_list = []
        
        for idx, df in enumerate(self.dfs):
            logger.info(f"Processing DataFrame {idx + 1}/{len(self.dfs)} with shape {df.shape}...")
            
            metadata = pd.DataFrame({
                "Column": df.columns,
                "Non-Null Count": df.notnull().sum().values,
                "Null Count": df.isnull().sum().values,
                "Unique Values": [df[col].nunique() for col in df.columns],
                "Data Type": df.dtypes.values
            })
            
            metadata_list.append(metadata)
            logger.info(f"Metadata for DataFrame {idx + 1} extracted: {metadata.shape[0]} columns processed.")
        
        logger.info("Metadata extraction completed for all DataFrames.")
        return metadata_list          # metadata_list is not used
