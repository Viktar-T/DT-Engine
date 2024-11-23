import os
import json
from datetime import datetime
import logging
from typing import Any, Dict
from src.config import METADATA_DIR

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class MetadataManager:
    """
    A class for dynamic metadata management in the data pipeline.
    """

    def __init__(self, pipeline_name: str = "data_pipeline", version: str = "v1.0"):
        """
        Initialize MetadataManager.

        Parameters:
        - pipeline_name (str): Name of the pipeline for versioning.
        - version (str): Version of the metadata file (e.g., 'v1.0').
        """
        self.pipeline_name = pipeline_name
        self.version = version
        self.metadata = {
            "pipeline_name": pipeline_name,
            "version": version,
            "start_time": str(datetime.now()),
            "steps": []
        }
        self.metadata_file = self._generate_metadata_filename()

    def _generate_metadata_filename(self) -> str:
        """
        Generate a versioned metadata filename.

        Returns:
        - str: Full path of the metadata file.
        """
        filename = f"metadata_{self.version}.json"
        return os.path.join(METADATA_DIR, filename)

    def add_step_metadata(self, step_name: str, details: Dict[str, Any]):
        """
        Add metadata for a specific step in the pipeline.

        Parameters:
        - step_name (str): Name of the pipeline step.
        - details (Dict[str, Any]): Metadata details for the step.
        """
        step_metadata = {
            "step_name": step_name,
            "timestamp": str(datetime.now()),
            "details": details
        }
        self.metadata["steps"].append(step_metadata)
        logger.info(f"Metadata updated for step: {step_name}")

    def finalize_metadata(self):
        """
        Finalize and save the metadata file.
        """
        self.metadata["end_time"] = str(datetime.now())
        with open(self.metadata_file, "w") as f:
            json.dump(self.metadata, f, indent=4)
        logger.info(f"Metadata saved to {self.metadata_file}")

    def visualize_metadata(self):
        """
        Print the metadata for visualization.
        """
        print(json.dumps(self.metadata, indent=4))
