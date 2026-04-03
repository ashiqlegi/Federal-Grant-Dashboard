import sys 
import os
import time
import logging

#  Logging setup

os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        # Handler 1: print to terminal
        logging.StreamHandler(sys.stdout),
        # Handler 2: write to a log file at the same time
        logging.FileHandler("logs/pipeline.log"),
    ]
)

logger = logging.getLogger("pipeline")

# Imports 
from etl.database import init_db
from etl.extract  import extract_awards
from etl.transform import transform_awards
from etl.load     import start_run, finish_run, load_awards, load_quality_flags


#  Main pipeline function 
def run_pipeline():
    """
    Every run is recorded in pipeline_runs regardless of
    whether it succeeds or fails. This gives us a full
    audit trail of everything that ever happened.
    """

    logger.info("=" * 50)
    logger.info("Pipeline starting")

    # Make sure the database and tables exist
       # wipe anything if the tables are already there
    init_db()

    # Record the start of this run and get back a run_id
    run_id = start_run()

    # Start the timer so we know how long the pipeline took
    start_time = time.time()

    try:
        # ── EXTRACT ───────────────────────────────────────────
        logger.info("Step 1/3 — Extracting data from API")
        raw_records = extract_awards()
        logger.info(f"Extracted {len(raw_records)} records")

        # ── TRANSFORM ─────────────────────────────────────────
        logger.info("Step 2/3 — Transforming and validating data")
        cleaned_records, quality_flags = transform_awards(raw_records)
        logger.info(f"Transformed {len(cleaned_records)} records")
        logger.info(f"Quality flags found: {len(quality_flags)}")

        #  LOAD 
        logger.info("Step 3/3 — Loading data into database")
        rows_loaded = load_awards(run_id, cleaned_records)
        load_quality_flags(run_id, quality_flags)

        #  FINISH 
        duration = time.time() - start_time
        finish_run(
            run_id,
            status="success",
            rows_extracted=len(raw_records),
            rows_loaded=rows_loaded,
            quality_flags=len(quality_flags),
            duration_sec=duration
        )

        logger.info(f"Pipeline completed successfully in {round(duration, 2)}s")
        logger.info("=" * 50)

    except Exception as e:
        # If ANYTHING goes wrong in any step, we land here.
        # We record the failure in the database so the dashboard
        # can show it, then re-raise so the error is still visible.
        duration = time.time() - start_time
        finish_run(
            run_id,
            status="failure",
            duration_sec=duration,
            error_message=str(e)
        )

        logger.error(f"Pipeline FAILED: {e}")
        logger.info("=" * 50)

        # sys.exit(1) tells the operating system the program failed
        # Exit code 1 = failure, exit code 0 = success
        # This matters later when scheduling the pipeline to run automatically
        sys.exit(1)


#  Entry point 
if __name__ == "__main__":
    run_pipeline()
