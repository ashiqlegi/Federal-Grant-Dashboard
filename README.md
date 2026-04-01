 Federal Contract Awards — ETL Pipeline & Monitor

A production-style ETL pipeline that extracts federal contract award data 
from the USASpending.gov REST API, transforms and validates it, loads it 
into a SQLite database, and surfaces it through a Flask monitoring dashboard.

## What it does

- Pulls live federal contract data from the USASpending.gov public API
- Cleans and validates every record, flagging data quality issues automatically
- Stores data across 3 relational tables with full run history tracking
- Displays pipeline health and contract records in a web dashboard

## Tech stack

| Layer        | Technology          |
|-------------|---------------------|
| Data source  | USASpending.gov REST API |
| Language     | Python 3            |
| Database     | SQLite              |
| Web server   | Flask               |
| Version control | Git / GitHub    |

## Project structure
```
etl_pipeline/
├── etl/
│   ├── database.py     # Schema definition and connection helper
│   ├── extract.py      # Pulls raw data from USASpending API
│   ├── transform.py    # Cleans records, flags quality issues
│   └── load.py         # Writes to SQLite across 3 tables
├── dashboard/
│   └── app.py          # Flask monitoring dashboard
├── logs/               # Auto-generated pipeline run logs
├── data/               # SQLite database (git-ignored)
├── pipeline.py         # Main orchestrator — runs full ETL
└── requirements.txt
```

## Database schema

Three tables work together to give full visibility into every pipeline run:

- `pipeline_runs` — metadata for every run (status, row counts, duration, errors)
- `awards` — cleaned federal contract records linked to the run that loaded them
- `data_quality_log` — flags for missing or malformed fields, per run

## How to run it

**1. Clone the repo**
```bash
git clone https://github.com/YOUR_USERNAME/etl-pipeline.git
cd etl-pipeline
```

**2. Create and activate a virtual environment**
```bash
python -m venv venv
venv\Scripts\activate        # Windows
source venv/bin/activate     # Mac/Linux
```

**3. Install dependencies**
```bash
pip install -r requirements.txt
```

**4. Initialize the database**
```bash
python etl/database.py
```

**5. Run the pipeline**
```bash
python pipeline.py
```

**6. Start the dashboard**
```bash
python dashboard/app.py
```
Then open http://localhost:5000 in your browser.

## Key concepts demonstrated

- **ETL architecture** — extract, transform, and load are fully separated 
  so each stage can be tested, debugged, and swapped independently
- **REST API integration** — POST requests with JSON payloads to a 
  public government API
- **Relational database design** — primary keys, foreign keys, and 
  constraints enforce data integrity automatically
- **Data quality tracking** — every run logs what was wrong and how often,
  giving analysts visibility into upstream data issues
- **Parameterized SQL queries** — all database writes use placeholders 
  to prevent SQL injection
- **Structured logging** — every run writes timestamped logs to both 
  the terminal and a persistent log file
- **Error handling** — pipeline failures are recorded to the database 
  with full error messages rather than silently crashing