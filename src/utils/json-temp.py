import json
import logging
from src.config import RAW_DATA_DIR

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Load the JSON file
file_path = 'data/data_templates/files_with_raw_data_links.json'
with open(file_path, 'r') as file:
    data = json.load(file)

logger.info("Original data loaded.")

# Define the template structure
template = {
    "main_file_name": "empty",
    "eco_file_name": "empty",
    "description": "empty",
    "diesel_test_type": "empty",
    "fuel": "diesel",
    "diesel_engine_name": "empty",
}

# Update each item to match the template structure
for category, items in data.items():
    for item in items:
        # Add missing keys
        for key in template:
            if key not in item:
                item[key] = template[key]
        # Remove extra keys
        keys_to_remove = [key for key in item if key not in template]
        for key in keys_to_remove:
            del item[key]

logger.info("Data updated with template structure.")

# Save the updated JSON file
with open(file_path, 'w') as file:
    json.dump(data, file, indent=4)

logger.info("Refactored JSON saved.")