import sqlite3
import os

# The path to the DB.
DB_PATH = os.path.join(os.path.dirname(__file__), '..''data', 'spending.db')

##SQLite schema for tracking pipeline runs and storing award data.

SCHEMA ="""

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_at TEXT NOT NULL,
    status TEXT NOT NULL,
    rows_extracted INTEGER DEFAULT 0,
    rows_loaded INTEGER DEFAULT 0,
    quality_flags INTEGER DEFAULT 0,
    duration_sec REAL DEFAULT 0, 
    error_message TEXT
    );

    CREATE TABLE IF NOT EXISTS awards (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL,
    award_id TEXT, 
    award_type TEXT,
    recipient_name TEXT,
    awarding_agency TEXT,
    award_amount REAL,
    start_date TEXT,
    end_date TEXT,
    loaded_at TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES pipeline_runs(id)
);
"""

def init_db():
    "creates the database file and tables if they don't exist yet."
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    #creating the SQLite file if it doesn't exist yet.
    conn = sqlite3.connect(DB_PATH)

    conn.executescript(SCHEMA)
    conn.commit()
    conn.close()
    print(f"Database initiated at {DB_PATH}")

#"Returns an open DB connection. Then call the .close() method on the connection when done."
def get_conn():
    return sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

if __name__ == "__main__":
    init_db()