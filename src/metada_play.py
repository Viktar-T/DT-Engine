from metadata_manager import MetadataManager
from src.config import RAW_DATA_DIR, PROCESSED_DATA_DIR, METADATA_DIR

def main():
    # Initialize the metadata manager
    manager = MetadataManager(METADATA_DIR)
    
    # Update metadata with some example data
    for i in range(5):
        manager.update_metadata(f'example_key_{i}', {'example_field': f'example_value_{i}'})
    
    # Print the current metadata
    print("Current Metadata:", manager.metadata)
    
    # Save the metadata
    # manager._save_metadata()

if __name__ == "__main__":
    main()
