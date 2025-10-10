"""Database utilities for saving survey results."""

import os
import polars as pl
import duckdb
from pathlib import Path


def get_database_connection(env: str = "DEV"):
    """
    Get database connection based on environment.

    Args:
        env: Environment type - "DEV" for local DuckDB, "PROD" for production database

    Returns:
        Database connection object
    """
    if env == "DEV":
        # Use local DuckDB file
        db_path = Path("data/survey_results.duckdb")
        db_path.parent.mkdir(parents=True, exist_ok=True)
        return duckdb.connect(str(db_path))
    elif env == "PROD":
        # For production, you would configure your production database here
        # Example for PostgreSQL:
        # import psycopg2
        # return psycopg2.connect(
        #     host=os.getenv("DB_HOST"),
        #     database=os.getenv("DB_NAME"),
        #     user=os.getenv("DB_USER"),
        #     password=os.getenv("DB_PASSWORD")
        # )
        raise NotImplementedError("Production database not configured yet")
    else:
        raise ValueError(f"Invalid environment: {env}. Must be 'DEV' or 'PROD'")


def save_to_database(
    results_df: pl.DataFrame,
    variable_df: pl.DataFrame,
    question_df: pl.DataFrame,
    table_prefix: str = "survey",
    env: str = "DEV",
    if_exists: str = "replace"
):
    """
    Save survey dataframes to database.

    Args:
        results_df: Survey results DataFrame
        variable_df: Variable metadata DataFrame
        question_df: Question metadata DataFrame
        table_prefix: Prefix for table names
        env: Environment type ("DEV" or "PROD")
        if_exists: What to do if table exists - "replace", "append", or "fail"
    """
    if env == "DEV":
        # Use DuckDB for local development
        conn = get_database_connection(env)

        try:
            # Create tables from Polars DataFrames
            # DuckDB can directly query Polars DataFrames

            # Save results table
            if if_exists == "replace":
                conn.execute(f"DROP TABLE IF EXISTS {table_prefix}_results")
            conn.execute(f"""
                CREATE TABLE {table_prefix}_results AS
                SELECT * FROM results_df
            """)

            # Get row count
            results_count = conn.execute(f"SELECT COUNT(*) FROM {table_prefix}_results").fetchone()[0]

            print(f"\n✓ Saved to DuckDB database:")
            print(f"  - {table_prefix}_results: {results_count} rows")
            print(f"  - Database location: {conn.execute('SELECT current_database()').fetchone()[0]}")

        finally:
            conn.close()

    elif env == "PROD":
        # For production databases, implement appropriate logic
        # Example with Polars write_database:
        # connection_uri = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
        # results_df.write_database(f"{table_prefix}_results", connection_uri, if_table_exists=if_exists)
        # variable_df.write_database(f"{table_prefix}_variables", connection_uri, if_table_exists=if_exists)
        # question_df.write_database(f"{table_prefix}_questions", connection_uri, if_table_exists=if_exists)
        raise NotImplementedError("Production database not configured yet")


def query_database(sql: str, env: str = "DEV") -> pl.DataFrame:
    """
    Execute SQL query and return results as Polars DataFrame.

    Args:
        sql: SQL query to execute
        env: Environment type ("DEV" or "PROD")

    Returns:
        Query results as Polars DataFrame
    """
    if env == "DEV":
        conn = get_database_connection(env)
        try:
            # DuckDB can return results as Polars DataFrame
            result = conn.execute(sql).pl()
            return result
        finally:
            conn.close()
    elif env == "PROD":
        raise NotImplementedError("Production database not configured yet")
