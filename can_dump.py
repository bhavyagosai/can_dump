import datetime
import os
import tarfile
import threading
import itertools
import sys
import time
import clickhouse_connect
import logging
import shutil

# ##############################################################
# USER CONFIGURATION SECTION
# ##############################################################

# ClickHouse server details
URL = ""
PORT = 80  # Default HTTP port for ClickHouse
INTERFACE = 'https'  # We are using https protocol to connect externally
USER = ""  # Replace with actual username
PASSWORD = ""  # Replace with actual password
DATABASE = ""
TABLE = "can_raw"
DATE_COLUMN = "timestamp"
DEVICE_ID = ""  # Replace with actual device id

# Directory where exported CSV files will be stored
# By default, set to the current directory.
# To set a custom path, uncomment and modify the following line:
# EXPORT_DIR = "/path/to/custom/directory"
# Current directory set as default
EXPORT_DIR = os.path.dirname(os.path.realpath(__file__))

# Define the date range for the export (YYYY, MM, DD, HH, MM, SS)
START_DATE = datetime.datetime(2024, 6, 28, 0, 0)  # Start at midnight
END_DATE = datetime.datetime(2024, 6, 29, 0, 0)  # End at midnight the next day

# ##############################################################
# END OF USER CONFIGURATION SECTION
# ##############################################################

# Setup logging to print to console without newline at the end


class CustomLogger(logging.StreamHandler):
    def emit(self, record):
        log_entry = self.format(record)
        sys.stdout.write(f"{log_entry}\n")
        sys.stdout.flush()


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()
for handler in logger.handlers:
    logger.removeHandler(handler)
logger.addHandler(CustomLogger(sys.stdout))

# Loader animation function


def loading_animation(stop_event, message):
    """Print loading animation in the console with a message."""
    sys.stdout.write(message + " ")
    sys.stdout.flush()
    for char in itertools.cycle('|/-\\'):
        if stop_event.is_set():
            break
        sys.stdout.write(char)
        sys.stdout.flush()
        time.sleep(0.1)
        sys.stdout.write('\b')
    sys.stdout.write('\n')


def export_data(client, current_datetime, temp_dir):
    """Export data from ClickHouse and save it as a CSV file."""
    FILE_NAME = f"can_raw_{current_datetime.strftime('%Y%m%d_%H%M')}.csv"
    EXPORT_PATH = os.path.join(temp_dir, FILE_NAME)

    formatted_date = current_datetime.strftime('%Y-%m-%d %H:%M:%S')
    next_day = (current_datetime + datetime.timedelta(days=1)
                ).strftime('%Y-%m-%d %H:%M:%S')
    query = (
        f"SELECT * FROM {DATABASE}.{TABLE} "
        f"WHERE {DATE_COLUMN} >= '{formatted_date}' AND {DATE_COLUMN} < '{next_day}' "
        f"AND id = '{DEVICE_ID}'"
    )
    fmt = 'CSVWithNames'  # Format for the output

    stop_event = threading.Event()
    loader_thread = threading.Thread(target=loading_animation, args=(
        stop_event, f"Exporting data for {formatted_date} to {next_day}"))

    logger.info(f"Sending data request for {formatted_date} to {next_day}")
    loader_thread.start()  # Start the loading animation

    try:
        stream = client.raw_stream(query=query, fmt=fmt)
        with open(EXPORT_PATH, "wb") as f:
            for chunk in stream:
                f.write(chunk)

        logger.info(f"\nExported file: {EXPORT_PATH} Size: {
                    os.path.getsize(EXPORT_PATH)} bytes")

    except Exception as e:
        logger.error(
            f"\nAn error occurred while trying to fetch data from ClickHouse: {str(e)}\n")
        raise

    finally:
        stop_event.set()
        loader_thread.join()


def main():
    """Main function to handle data export process."""
    try:
        client = clickhouse_connect.get_client(
            host=URL, port=80, interface='https', username=USER, password=PASSWORD, database=DATABASE)
    except Exception as e:
        logger.error(f"\nFailed to create ClickHouse client: {str(e)}\n")
        sys.exit(1)

    temp_dir = os.path.join(EXPORT_DIR, f"DEVICE_{DEVICE_ID}_{
        START_DATE.strftime('%d_%b')}_to_{END_DATE.strftime('%d_%b')}_RAW_CAN_DATA")
    os.makedirs(temp_dir, exist_ok=True)
    logger.info(f"\n\nCreated temporary directory: {temp_dir}\n")

    try:
        current_datetime = START_DATE
        while current_datetime < END_DATE:
            export_data(client, current_datetime, temp_dir)
            current_datetime += datetime.timedelta(days=1)

        tar_file_path = os.path.join(EXPORT_DIR, f"DEVICE_{DEVICE_ID}_{
                                     START_DATE.strftime('%d_%b')}_to_{END_DATE.strftime('%d_%b')}_RAW_CAN_DATA.tar.gz")

        stop_event = threading.Event()
        loader_thread = threading.Thread(target=loading_animation, args=(
            stop_event, "Compressing data into tar file"))
        logger.info(f"\nStarting compression into tar file: {tar_file_path}\n")
        loader_thread.start()

        with tarfile.open(tar_file_path, "w:gz") as tar:
            tar.add(temp_dir, arcname=os.path.basename(temp_dir))

        stop_event.set()
        loader_thread.join()

        logger.info(f"Compressed all CSV files into: {tar_file_path}\n")

    except Exception as e:
        logger.error(f"\nAn error occurred during data export: {str(e)}\n")
        sys.exit(1)
    finally:
        try:
            client.close()
        except Exception as e:
            logger.error(f"\nFailed to close ClickHouse client: {str(e)}\n")

        # Clean up the temporary directory
        try:
            shutil.rmtree(temp_dir)
            logger.info(f"Deleted temporary directory: {temp_dir}\n")
        except Exception as e:
            logger.error(f"\nFailed to delete temporary directory: {str(e)}\n")

        logger.info("\nData export and upload complete.\n")


if __name__ == "__main__":
    main()
