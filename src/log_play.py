
from src.log_manager import LogManager
from src.config import LOGS_DIR

def main():
    # Initialize the LogManager
    log_manager = LogManager(logs_dir=LOGS_DIR)
    
    # Log some messages
    log_manager.log_info("This is an info message.")
    log_manager.log_warning("This is a warning message.")
    log_manager.log_error("This is an error message.")
    log_manager.log_debug("This is a debug message.")

    print(f"Logs have been written to {log_manager.log_file}")

if __name__ == "__main__":
    main()