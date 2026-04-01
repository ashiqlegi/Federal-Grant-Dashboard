import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from flask import Flask, render_template_string
from etl.database import get_conn

# Flask(__name__) creates the web application
app = Flask(__name__)

# ── SQL QUERIES ────────────────────────────────────────────────
# These are the questions we ask our database to power the dashboard

def get_pipeline_runs():
    """Get all pipeline runs, newest first."""
    conn = get_conn()
    runs = conn.execute("""
        SELECT 
            id,
            run_at,
            status,
            rows_extracted,
            rows_loaded,
            quality_flags,
            round(duration_sec, 2) as duration_sec,
            error_message
        FROM pipeline_runs
        ORDER BY run_at DESC
    """).fetchall()
    conn.close()
    return [dict(r) for r in runs]

def get_latest_awards():
    """Get the 20 most recently loaded awards."""
    conn = get_conn()
    awards = conn.execute("""
        SELECT
            a.award_id,
            a.recipient_name,
            a.awarding_agency,
            a.award_amount,
            a.start_date,
            a.end_date,
            a.description
        FROM awards a
        INNER JOIN pipeline_runs p ON a.run_id = p.id
        WHERE p.status = 'success'
        ORDER BY a.loaded_at DESC
        LIMIT 20
    """).fetchall()
    conn.close()
    return [dict(a) for a in awards]

def get_summary_stats():
    """Get top level numbers for the metric cards."""
    conn = get_conn()
    stats = conn.execute("""
        SELECT
            COUNT(*)                                    as total_runs,
            SUM(CASE WHEN status = 'success' THEN 1 
                     ELSE 0 END)                        as successful_runs,
            SUM(rows_loaded)                            as total_rows,
            round(AVG(duration_sec), 2)                 as avg_duration
        FROM pipeline_runs
    """).fetchone()
    conn.close()
    return dict(stats)


# ── ROUTES ─────────────────────────────────────────────────────
# A route is a URL that Flask listens for.
# When the browser visits that URL, Flask runs the function below it.

@app.route("/")
def dashboard():
    """Main dashboard page."""
    runs   = get_pipeline_runs()
    awards = get_latest_awards()
    stats  = get_summary_stats()
    return render_template_string(HTML, runs=runs, awards=awards, stats=stats)


# ── HTML TEMPLATE ──────────────────────────────────────────────
# render_template_string() lets us write HTML directly in Python.
# The {{ }} syntax is how we inject Python variables into the HTML.

HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>ETL Pipeline Monitor</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: system-ui, sans-serif; background: #0d1117; color: #e6edf3; padding: 24px; }
  h1 { font-size: 18px; font-weight: 500; margin-bottom: 4px; }
  .sub { font-size: 13px; color: #7d8590; margin-bottom: 24px; }
  .metrics { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-bottom: 24px; }
  .metric { background: #161b22; border: 0.5px solid #30363d; border-radius: 8px; padding: 16px; }
  .metric-label { font-size: 11px; color: #7d8590; margin-bottom: 6px; }
  .metric-value { font-size: 26px; font-weight: 500; }
  .blue { color: #58a6ff; } .green { color: #3fb950; }
  .amber { color: #d29922; } .purple { color: #bc8cff; }
  .section { background: #161b22; border: 0.5px solid #30363d; border-radius: 8px; padding: 16px; margin-bottom: 16px; }
  .section-title { font-size: 13px; font-weight: 500; color: #c9d1d9; margin-bottom: 14px; }
  table { width: 100%; border-collapse: collapse; font-size: 12px; }
  th { text-align: left; color: #7d8590; font-weight: 400; padding: 8px 10px; border-bottom: 0.5px solid #30363d; }
  td { padding: 8px 10px; border-bottom: 0.5px solid #21262d; color: #c9d1d9; }
  tr:last-child td { border-bottom: none; }
  .pill { display: inline-block; font-size: 11px; padding: 2px 8px; border-radius: 10px; }
  .success { background: #1f2a1f; color: #3fb950; }
  .failure { background: #2a1f1f; color: #f85149; }
  .running { background: #1f2331; color: #58a6ff; }
  .pulse { display: inline-block; width: 8px; height: 8px; border-radius: 50%; background: #3fb950; margin-right: 8px; animation: pulse 2s infinite; }
  @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: .3; } }
  .amount { color: #3fb950; font-family: monospace; }
  .refresh { float: right; font-size: 11px; padding: 4px 12px; border: 0.5px solid #30363d; background: transparent; color: #7d8590; border-radius: 6px; cursor: pointer; text-decoration: none; }
  .refresh:hover { background: #21262d; color: #c9d1d9; }
</style>
</head>
<body>

<div style="display:flex; align-items:center; justify-content:space-between; margin-bottom:4px">
  <h1><span class="pulse"></span>ETL Pipeline Monitor</h1>
  <a href="/" class="refresh">Refresh</a>
</div>
<div class="sub">USASpending.gov — Federal contract awards — SQLite backend</div>

<!-- METRIC CARDS -->
<div class="metrics">
  <div class="metric">
    <div class="metric-label">Total runs</div>
    <div class="metric-value blue">{{ stats.total_runs }}</div>
  </div>
  <div class="metric">
    <div class="metric-label">Successful runs</div>
    <div class="metric-value green">{{ stats.successful_runs }}</div>
  </div>
  <div class="metric">
    <div class="metric-label">Total rows loaded</div>
    <div class="metric-value amber">{{ stats.total_rows }}</div>
  </div>
  <div class="metric">
    <div class="metric-label">Avg duration</div>
    <div class="metric-value purple">{{ stats.avg_duration }}s</div>
  </div>
</div>

<!-- PIPELINE RUN HISTORY -->
<div class="section">
  <div class="section-title">Pipeline run history</div>
  <table>
    <thead>
      <tr>
        <th>Run ID</th>
        <th>Started at</th>
        <th>Status</th>
        <th>Extracted</th>
        <th>Loaded</th>
        <th>Quality flags</th>
        <th>Duration</th>
        <th>Error</th>
      </tr>
    </thead>
    <tbody>
      {% for run in runs %}
      <tr>
        <td style="color:#58a6ff">#{{ run.id }}</td>
        <td>{{ run.run_at[:19] }}</td>
        <td><span class="pill {{ run.status }}">{{ run.status }}</span></td>
        <td>{{ run.rows_extracted }}</td>
        <td>{{ run.rows_loaded }}</td>
        <td>{{ run.quality_flags }}</td>
        <td>{{ run.duration_sec }}s</td>
        <td style="color:#f85149">{{ run.error_message or '' }}</td>
      </tr>
      {% endfor %}
    </tbody>
  </table>
</div>

<!-- LATEST RECORDS -->
<div class="section">
  <div class="section-title">Latest contract awards loaded</div>
  <table>
    <thead>
      <tr>
        <th>Award ID</th>
        <th>Recipient</th>
        <th>Agency</th>
        <th>Amount</th>
        <th>Start date</th>
        <th>End date</th>
      </tr>
    </thead>
    <tbody>
      {% for award in awards %}
      <tr>
        <td style="font-family:monospace; font-size:11px; color:#58a6ff">{{ award.award_id }}</td>
        <td>{{ award.recipient_name }}</td>
        <td>{{ award.awarding_agency }}</td>
        <td class="amount">${{ "{:,.0f}".format(award.award_amount or 0) }}</td>
        <td>{{ award.start_date }}</td>
        <td>{{ award.end_date }}</td>
      </tr>
      {% endfor %}
    </tbody>
  </table>
</div>

</body>
</html>
"""

# ── START THE SERVER ───────────────────────────────────────────
if __name__ == "__main__":
    # debug=True means Flask will auto-reload when you save changes
    # Never use debug=True in production — only for development
    print("Dashboard running at http://localhost:5000")
    app.run(debug=True)