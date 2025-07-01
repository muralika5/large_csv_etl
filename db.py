import sqlite3
from config import DB_PATH

def create_table():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(f'''
        CREATE TABLE IF NOT EXISTS transactions (
            transaction_id TEXT PRIMARY KEY,
            user_id TEXT,
            amount REAL,
            timestamp TEXT,
            status TEXT,
            processed_at datetime
        )
    ''')
    conn.commit()
    conn.close()
    # we can also have another table to store file metadata and have a file id in transactions table. Considering that out of scope