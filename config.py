import os
from datetime import datetime, timezone, timedelta


# Path to the final processed CSV file
PROCESSED_CSV_PATH = 'processed_output.csv'

# Path to the SQLite database file
DB_PATH =  os.getenv('DB_PATH', 'etl_database.db')

# Chunk size for reading the CSV
CHUNK_SIZE = 100_000

# Directory to store temporary chunk files
TEMP_DIR = 'temp_chunks'

# Create and makes srue temp directory exists
os.makedirs(TEMP_DIR, exist_ok=True)

IST = timezone(timedelta(hours=5, minutes=30))
