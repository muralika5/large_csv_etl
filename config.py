import os
from datetime import timedelta, timezone


# Path to the SQLite database file
DB_PATH = os.getenv('DB_PATH', 'etl_database.db')

# Chunk size for reading the CSV
CHUNK_SIZE = 100_000

# Directory to store temporary chunk files
TEMP_DIR = 'temp_chunks'

OUTPUT_DIR = 'output'

OUTPUT_CSV_PATH = f'{OUTPUT_DIR}/output.csv'


# Create and makes srue temp directory exists
os.makedirs(TEMP_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

IST = timezone(timedelta(hours=5, minutes=30))
