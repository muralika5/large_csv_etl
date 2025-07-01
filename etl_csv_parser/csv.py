import logging
import os
from datetime import datetime

import pandas as pd

from config import CHUNK_SIZE, IST, TEMP_DIR

logger = logging.getLogger(__name__)


class CSVProcessor:
    def __init__(self, input_csv, chunk_size=CHUNK_SIZE, temp_dir=TEMP_DIR):
        self.input_csv = input_csv
        self.chunk_size = chunk_size
        self.temp_dir = temp_dir
        os.makedirs(self.temp_dir, exist_ok=True)

    def validate_row(self, row):
        try:
            status = str(row['status']).lower()
            amount = float(row['amount'])
            # todo: can add more validations like checking if transaction id is unique or not querying the db
            if amount < 0 or status == 'cancelled':
                return False
            return True
        except Exception as e:
            logger.error(f"[CSVProcessor] Skipping row due to error: {e}")
            return False

    def clean_chunk(self, chunk):
        # Filter valid rows using validate_row
        valid_mask = chunk.apply(self.validate_row, axis=1)
        valid_chunk = chunk[valid_mask].copy()
        if valid_chunk.empty:
            return valid_chunk
        # Clean: lowercase status, add processed_at
        valid_chunk['status'] = valid_chunk['status'].str.lower()
        valid_chunk['processed_at'] = datetime.now(IST).isoformat()
        return valid_chunk

    def process_chunk(self, chunk, chunk_idx):
        try:
            cleaned = self.clean_chunk(chunk)
            if cleaned.empty:
                logger.info(f"[CSVProcessor] Chunk {chunk_idx} is empty after cleaning. Skipping.")
                return None
            temp_file = os.path.join(self.temp_dir, f'chunk_{chunk_idx}.csv')
            cleaned.to_csv(temp_file, index=False)
            logger.info(f"[CSVProcessor] Processed and wrote chunk {chunk_idx} to {temp_file} ({len(cleaned)} rows)")
            return temp_file
        except Exception as e:
            logger.error(f"[CSVProcessor] Error processing chunk {chunk_idx}: {e}")
            return None

    def read_csv(self):
        return pd.read_csv(self.input_csv, chunksize=self.chunk_size)

    def concatenate(self, output_file, temp_files=None):
        with open(output_file, 'w') as fout:
            header_written = False
            for temp_file in temp_files:
                with open(temp_file, 'r') as fin:
                    if not header_written:
                        fout.write(fin.readline())
                        header_written = True
                    else:
                        fin.readline()
                    for line in fin:
                        fout.write(line)
        logger.info(f"[CSVProcessor] Concatenated {len(temp_files)} temp files into {output_file}")
