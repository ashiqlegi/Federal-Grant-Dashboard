import sqlite3from datetime import datetime, timezone
import sys
import os

#importing the get_conn function from a sibling folder.
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from etl.database import get_conn

#Function to record the start of a pipeline run. Will return run_id.
def start_run():
    conn = getconn()

    now = datetime.now(timezone.utc).isoformat()

    cursor = conn.execute(
        "INSERT INTO pipeline_runs (run_at, status) VALUES (?, ?)",
        (now, "running")
    )

    conn.commit()

    run_id = cursor.lastrowid

    conn.close()
    print(f"Pipeline run #{run_id} started at {now}")

    def finish_run(run_id, status, rows_extracted=0, rows_loaded=0, quality_flags=0, error_message=None, duration_sec=0):
        conn = get_conn()

        now = datetime.now(timezone.utc).isoformat()

        conn.execute(
            """
            UPDATE pipeline_runs
            SET status = ?, rows_extracted = ?, rows_loaded = ?, quality_flags = ?, error_message = ?, duration_sec = ?
            WHERE id = ?
            """,
            (status, rows_extracted, rows_loaded, quality_flags, error_message, duration_sec, run_id)
        )

        conn.commit()
        conn.close()
        print(f"Pipeline run #{run_id} finished with status '{status}' at {now}")

def load_awards(run_id, cleaned_records):