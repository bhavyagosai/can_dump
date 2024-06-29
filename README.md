# ClickHouse Exporter CAN dump

This repository contains a script to export data from ClickHouse and compress it into a tar.gz file.

## Configuration

Before running the script, you need to configure the `can_dump.py` file with your ClickHouse server details, date range, and export directory.

### Configuration Values

Open `export_data.py` and modify the following section:

```python
# ClickHouse server details
URL = ""  # URL for where your database is hosted
USER = ""  # Replace with actual username
PASSWORD = ""  # Replace with actual password
DATABASE = ""  # Replace with name of the database inside clickhouse
TABLE = "can_raw"  # Replace with name of the table inside clickhouse
DATE_COLUMN = "timestamp"
DEVICE_ID = ""  # Replace with actual device id

# Directory where exported CSV files will be stored
# By default, set to the current directory. 
# To set a custom path, uncomment and modify the following line:
# EXPORT_DIR = "/path/to/custom/directory"
EXPORT_DIR = os.path.dirname(os.path.realpath(__file__))  # Current directory set as default

# Define the date range for the export (YYYY, MM, DD, HH, MM, SS)
START_DATE = datetime.datetime(2024, 6, 28, 0, 0)  # Start at midnight
END_DATE = datetime.datetime(2024, 6, 29, 0, 0)  # End at midnight the next day
```

### How to run

After setting up the config for `can_dump.py`, run `script.sh`. If it is not executable, make it using the following command

```bash
chmod +x script.sh
```
