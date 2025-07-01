import sqlite3

import pandas as pd

from config import DB_PATH


class DatabaseLoader:
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path

    def batch_insert(self, processed_csv, batch_size=1000):
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        for chunk in pd.read_csv(processed_csv, chunksize=batch_size):
            tuples = [tuple(row) for row in chunk.values]
            # Assuming data in the file are in its final state and will not be updated, to acheive idempotency,
            # we shall ignore the duplicate entries.
            # Hence not committing the transaction in an atomic block
            cur.executemany(
                '''
                INSERT OR IGNORE INTO transactions (transaction_id, user_id, amount, timestamp, status, processed_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ''',
                tuples
            )
            conn.commit()
        conn.close()
