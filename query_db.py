#!/usr/bin/env python
"""Simple DuckDB query helper."""

import sys
import duckdb
from pathlib import Path

db_path = Path("data/survey_results.duckdb")

if not db_path.exists():
    print(f"Error: Database not found at {db_path}")
    sys.exit(1)

conn = duckdb.connect(str(db_path))

# If SQL query provided as argument, execute it
if len(sys.argv) > 1:
    sql = " ".join(sys.argv[1:])
    result = conn.execute(sql).fetchall()
    for row in result:
        print(row)
else:
    # Interactive mode - show tables and sample query
    print("Available tables:")
    tables = conn.execute("SHOW TABLES").fetchall()
    for table in tables:
        print(f"  - {table[0]}")

    print("\nExample queries:")
    print("  uv run python query_db.py 'SELECT * FROM u25_survey_results LIMIT 5'")
    print("  uv run python query_db.py 'SELECT COUNT(*) FROM u25_survey_results'")
    print("  uv run python query_db.py 'DESCRIBE u25_survey_results'")

conn.close()
