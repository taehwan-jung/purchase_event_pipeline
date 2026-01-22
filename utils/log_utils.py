
import logging
import os 

def setup_logger(name, log_file):
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Prevent duplicate handlers if already exists
    if logger.handlers:
        return logger

    # Set output format
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Console output handler
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    # File save handler
    if log_file:
    # Create log directory
        os.makedirs(os.path.dirname(log_file), exist_ok= True)

        file_handler = logging.FileHandler(log_file, encoding= 'utf-8')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger

     