import sqlite3
import pandas as pd
from config import DB_PATH


class DatabaseLoader:
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path


    def batch_insert(self, processed_csv, batch_size=10000):
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        for chunk in pd.read_csv(processed_csv, chunksize=batch_size):
            tuples = [tuple(row) for row in chunk.values]
            cur.executemany(
                f'''INSERT OR IGNORE INTO transactions (transaction_id, user_id, amount, timestamp, status, processed_at)
                    VALUES (?, ?, ?, ?, ?, ?)''',
                tuples
            )
            conn.commit()
        conn.close()
