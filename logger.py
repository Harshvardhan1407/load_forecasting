import logging
from logging.handlers import RotatingFileHandler

# Create a logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # Set the logging level to capture all messages

# Define a formatter
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(filename)s:%(lineno)s - %(message)s')

# Add a RotatingFileHandler to manage log file size
rotating_handler = RotatingFileHandler(
    'logs/load_forecasting.log',  # Log file name
    maxBytes = 10 * 1024 * 1024,  # Maximum file size: 10 MB
    backupCount = 5   # Keep 3 backup log files
)
rotating_handler.setFormatter(formatter)
rotating_handler.setLevel(logging.DEBUG)  # File handler captures all messages

# # Add a basic StreamHandler to log messages to the console
# console_handler = logging.StreamHandler()
# console_handler.setFormatter(formatter)
# console_handler.setLevel(logging.INFO)  # Console handler captures INFO and above

# Add handlers to the logger
logger.addHandler(rotating_handler)
# logger.addHandler(console_handler)

# Test the logger
if __name__ == "__main__":
    logger.debug("This is a DEBUG message")
    logger.info("This is an INFO message")
    logger.warning("This is a WARNING message")
    logger.error("This is an ERROR message")
    logger.critical("This is a CRITICAL message")
