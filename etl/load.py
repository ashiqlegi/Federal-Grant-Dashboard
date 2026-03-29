import sqlite3 
from datetime import datetime, timezone
import sys
import os

#importing the get_conn function from a sibling folder.
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from etl.database import get_conn

#Function to record the start of a pipeline run. Will return run_id.
def start_run():
    conn = get_conn()

    now = datetime.now(timezone.utc).isoformat()

    cursor = conn.execute(
        "INSERT INTO pipeline_runs (run_at, status) VALUES (?, ?)",
        (now, "running")
    )

    conn.commit()

    run_id = cursor.lastrowid

    conn.close()
    print(f"Pipeline run #{run_id} started at {now}")
    return run_id 

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


#Function to load the cleaned award data into the awards table. Each row has a run_id so we know which pipeline run created it.
def load_awards(run_id, cleaned_records):
    now = datetime.now(timezone.utc).isoformat()
    conn = get_conn()
    
    conn.executemany(
        """INSERT INTO awards (
        run_id, award_id, award_type, recipient_name, awarding_agency, award_amount, start_date, end_date, description, loaded_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", 
        [
            (
               run_id,
                r["award_id"],
                r["award_type"],
                r["recipient_name"],
                r["awarding_agency"],
                r["award_amount"],
                r["start_date"],
                r["end_date"],
                r["description"],
                now 
            )
            for r in cleaned_records
        ]
    )

    conn.commit()
    conn.close()
    print(f"Loaded { len(cleaned_records)} awards into database")
    return len(cleaned_records)

#Inserts data quality flags into data_quality_log.
def load_quality_flags(run_id, quality_flags):
    if not quality_flags:
        print("No quality flags to log")
        return
    
    conn = get_conn()

    conn.executemany(
        """INSERT INTO data_quality_log(run_id, field, issue, count)
        VALUES (?, ?, ?, ?)
        """,
        [
            (run_id, f["field"], f["issue"], f["count"])
            for f in quality_flags
        ]
    
    )
    conn.commit()
    conn.close()
    print(f"Logged {len(quality_flags)} quality flags")
if __name__ == "__main__":
    import sys 
    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
    from etl.extract import extract_awards
    from etl.transform import transform_awards
import time

start_time = time.time()

    # Start the run and get an id
run_id = start_run()

    # Run extract and transform
raw = extract_awards()
cleaned, flags = transform_awards(raw)

    # Load both into the database
rows = load_awards(run_id, cleaned)
load_quality_flags(run_id, flags)

    # Mark the run as finished
duration = time.time() - start_time
finish_run(
    run_id,
    status="success",
    rows_extracted=len(raw),
    rows_loaded=rows,
    quality_flags=len(flags),
    duration_sec=duration
)
print(f"\nDone! Full pipeline completed in {round(duration, 2)} seconds")