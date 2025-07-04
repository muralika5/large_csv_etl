import os
import sqlite3

import pandas as pd

from etl_csv_parser.csv import CSVProcessor
from etl_csv_parser.db_loader import DatabaseLoader
from etl_csv_parser.etl_pipeline import ETLPipeline


class TestETLPipeline:
    sample_csv = 'tests/sample_input.csv'
    output_csv = 'tests/test_output.csv'
    temp_db = 'test_etl.db'
    table_name = 'transactions'

    def setup_method(self):
        # Setup: ensure no leftover files
        if os.path.exists(self.output_csv):
            os.remove(self.output_csv)
        if os.path.exists(self.temp_db):
            os.remove(self.temp_db)
        # Setup DB connection
        self.conn = sqlite3.connect(self.temp_db)
        self.cur = self.conn.cursor()
        # Create table
        self.cur.execute(f'''
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                transaction_id TEXT PRIMARY KEY,
                user_id TEXT,
                amount REAL,
                timestamp TEXT,
                status TEXT,
                processed_at TEXT
            )
        ''')
        self.conn.commit()

    def teardown_method(self):
        # Teardown: clean up output file and DB
        if os.path.exists(self.output_csv):
            os.remove(self.output_csv)
        # Truncate table if exists and close DB connection
        try:
            self.cur.execute(f'DELETE FROM {self.table_name}')
            self.conn.commit()
        except Exception:
            pass
        self.cur.close()
        self.conn.close()
        if os.path.exists(self.temp_db):
            os.remove(self.temp_db)

    def test_clean_chunk(self):
        df = pd.read_csv(self.sample_csv)
        processor = CSVProcessor(self.sample_csv)
        cleaned = processor.clean_chunk(df)
        # Only rows 1 and 4 should remain (amount >= 0 and status != 'cancelled')
        assert len(cleaned) == 2
        assert set(cleaned['status']) == {'success', 'pending'}
        assert 'processed_at' in cleaned.columns

    def test_db_insert(self):
        # Prepare output CSV by cleaning sample
        df = pd.read_csv(self.sample_csv)
        processor = CSVProcessor('dummy.csv')
        cleaned = processor.clean_chunk(df)
        cleaned.to_csv(self.output_csv, index=False)

        # Insert using DatabaseLoader
        loader = DatabaseLoader(self.temp_db)
        loader.batch_insert(self.output_csv)
        # Check row count (should be 2: only valid rows from sample_input.csv)
        self.cur.execute(f'SELECT COUNT(*) FROM {self.table_name}')
        count = self.cur.fetchone()[0]
        assert count == 2

    def test_etl_pipeline(self):
        pipeline = ETLPipeline(
            csv_path=self.sample_csv,
            db_loader=DatabaseLoader(self.temp_db)
        )
        pipeline.run(output_csv_path=self.output_csv)
        # Check if output CSV was created
        assert os.path.exists(self.output_csv)
        # Check row count in output CSV
        output_df = pd.read_csv(self.output_csv)
        assert len(output_df) == 2
        # Check if data was inserted into DB
        self.cur.execute(f'SELECT COUNT(*) FROM {self.table_name}')
        count = self.cur.fetchone()[0]
        assert count == 2
        # Check if processed_at column exists
        assert 'processed_at' in output_df.columns
