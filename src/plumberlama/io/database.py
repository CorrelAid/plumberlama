import os
from typing import Optional

import polars as pl
from sqlalchemy import Column, MetaData, Table, create_engine

from plumberlama.config import Config
from plumberlama.logging_config import get_logger
from plumberlama.type_mapping import polars_to_sqlalchemy

logger = get_logger(__name__)


def _create_table_from_dataframe(
    df: pl.DataFrame, table_name: str, metadata: MetaData
) -> Table:
    """Create SQLAlchemy Table object from Polars DataFrame schema."""
    columns = []
    for col_name, dtype in zip(df.columns, df.dtypes):
        sqlalchemy_type = polars_to_sqlalchemy(dtype)
        columns.append(Column(col_name, sqlalchemy_type))

    return Table(table_name, metadata, *columns)


def save_to_database(
    results_df: pl.DataFrame,
    metadata_df: pl.DataFrame,
    table_prefix: str = "survey",
    append: bool = True,
    config: Optional[Config] = None,
):
    """Save survey dataframes to PostgreSQL database with native types."""

    connection_uri = config.get_db_connection_uri()
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
        if not append:
            # Create tables, fail if they already exist
            db_metadata.create_all(conn, tables=[results_table, metadata_table])

        results_records = results_df.to_dicts()
        conn.execute(results_table.insert(), results_records)

        metadata_records = metadata_df.to_dicts()
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
    connection_uri = config.get_db_connection_uri()

    # Use read_database_uri with connectorx (requires postgresql:// format)
    # Convert from postgresql+psycopg2:// to postgresql://
    cx_uri = connection_uri.replace("postgresql+psycopg2://", "postgresql://")

    result_df = pl.read_database_uri(sql, cx_uri)

    return result_df
