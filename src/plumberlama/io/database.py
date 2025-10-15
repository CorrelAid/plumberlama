"""Database utilities for saving survey results.

Uses SQLAlchemy and psycopg2 to explicitly create PostgreSQL tables with native types,
including ARRAY for list columns.
"""

import os
from typing import Optional

import polars as pl
from sqlalchemy import Column, MetaData, Table, create_engine

from plumberlama.config import Config
from plumberlama.logging_config import get_logger
from plumberlama.type_mapping import polars_to_sqlalchemy

logger = get_logger(__name__)


def get_connection_uri(config: Optional[Config] = None) -> str:
    """Get PostgreSQL connection URI.

    Args:
        config: Optional Config object. If not provided, reads from environment variables.

    Returns:
        Connection URI string for PostgreSQL
    """
    if config:
        return config.get_db_connection_uri()

    # Fallback to environment variables for backward compatibility
    db_user = os.getenv("DB_USER", "plumberlama")
    db_password = os.getenv("DB_PASSWORD", "plumberlama_dev")
    db_host = os.getenv("DB_HOST", "localhost")
    db_name = os.getenv("DB_NAME", "survey_data")
    db_port = os.getenv("DB_PORT", "5432")

    return (
        f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    )


def _create_table_from_dataframe(
    df: pl.DataFrame, table_name: str, metadata: MetaData
) -> Table:
    """Create SQLAlchemy Table object from Polars DataFrame schema.

    Args:
        df: Polars DataFrame
        table_name: Name for the table
        metadata: SQLAlchemy MetaData object

    Returns:
        SQLAlchemy Table object
    """
    columns = []
    for col_name, dtype in zip(df.columns, df.dtypes):
        sqlalchemy_type = polars_to_sqlalchemy(dtype)
        # All columns are nullable by default
        columns.append(Column(col_name, sqlalchemy_type, nullable=True))

    return Table(table_name, metadata, *columns)


def save_to_database(
    results_df: pl.DataFrame,
    metadata_df: pl.DataFrame,
    table_prefix: str = "survey",
    append: bool = True,
    config: Optional[Config] = None,
):
    """Save survey dataframes to PostgreSQL database with native types.

    Creates tables with explicit PostgreSQL types including ARRAY for list columns.
    Uses psycopg2 for efficient bulk insert.

    Note:
        All Polars types are handled natively:
        - Scalar types (Int64, String, Boolean, etc.) → PostgreSQL native types
        - pl.List types → PostgreSQL ARRAY types
        No preprocessing needed since we no longer use pl.Object/pl.Struct types.

    Args:
        results_df: Survey results DataFrame
        metadata_df: Variable-level metadata DataFrame with question info
        table_prefix: Prefix for table names
        append: If True, append to existing tables; if False, create new tables (fails if exist)
        config: Optional Config object for database connection. If not provided, uses environment variables.
    """
    connection_uri = get_connection_uri(config)
    engine = create_engine(connection_uri)
    db_metadata = MetaData()

    # Define table names
    results_table_name = f"{table_prefix}_results"
    metadata_table_name = f"{table_prefix}_metadata"

    # Create table schemas from DataFrames
    results_table = _create_table_from_dataframe(
        results_df, results_table_name, db_metadata
    )
    metadata_table = _create_table_from_dataframe(
        metadata_df, metadata_table_name, db_metadata
    )

    with engine.begin() as conn:
        # Create tables if they don't exist (or fail if append=False and they exist)
        if append:
            # Create tables only if they don't exist
            db_metadata.create_all(
                conn, tables=[results_table, metadata_table], checkfirst=True
            )
        else:
            # Create tables, fail if they already exist
            db_metadata.create_all(conn, tables=[results_table, metadata_table])

        # Insert data using bulk insert
        if len(results_df) > 0:
            results_records = results_df.to_dicts()
            # Convert any remaining dict values to strings (shouldn't happen but safety check)
            for record in results_records:
                for key, value in list(record.items()):
                    if isinstance(value, dict):
                        record[key] = str(value)
            conn.execute(results_table.insert(), results_records)

        if len(metadata_df) > 0:
            metadata_records = metadata_df.to_dicts()
            # Convert any remaining dict values to strings (shouldn't happen but safety check)
            for record in metadata_records:
                for key, value in list(record.items()):
                    if isinstance(value, dict):
                        record[key] = str(value)
            conn.execute(metadata_table.insert(), metadata_records)

    # Log confirmation
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "survey_data")

    logger.info("✓ Saved to PostgreSQL database:")
    logger.info(f"  - {results_table_name}: {len(results_df)} rows")
    logger.info(f"  - {metadata_table_name}: {len(metadata_df)} rows")
    logger.info(f"  - Database location: {db_host}:{db_port}/{db_name}")


def query_database(sql: str, config: Optional[Config] = None) -> pl.DataFrame:
    """Execute SQL query and return results as Polars DataFrame.

    Uses connectorx for efficient reading. PostgreSQL ARRAY types are automatically
    converted to Polars List types.

    Args:
        sql: SQL query to execute
        config: Optional Config object for database connection. If not provided, uses environment variables.

    Returns:
        Query results as Polars DataFrame
    """
    connection_uri = get_connection_uri(config)

    # Use read_database_uri with connectorx (requires postgresql:// format)
    # Convert from postgresql+psycopg2:// to postgresql://
    cx_uri = connection_uri.replace("postgresql+psycopg2://", "postgresql://")

    result_df = pl.read_database_uri(sql, cx_uri)

    return result_df
