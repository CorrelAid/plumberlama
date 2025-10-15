"""Database utilities for saving survey results.

Uses SQLAlchemy and psycopg2 to explicitly create PostgreSQL tables with native types,
including ARRAY for list columns.
"""

import os

import polars as pl
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    Integer,
    MetaData,
    Table,
    Text,
    create_engine,
)
from sqlalchemy.dialects.postgresql import ARRAY

from plumberlama.logging_config import get_logger

logger = get_logger(__name__)


def get_connection_uri() -> str:
    """Get PostgreSQL connection URI from environment variables."""
    db_user = os.getenv("DB_USER", "plumberlama")
    db_password = os.getenv("DB_PASSWORD", "plumberlama_dev")
    db_host = os.getenv("DB_HOST", "localhost")
    db_name = os.getenv("DB_NAME", "survey_data")
    db_port = os.getenv("DB_PORT", "5432")

    return (
        f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    )


def _polars_to_sqlalchemy_type(polars_dtype):
    """Convert Polars data type to SQLAlchemy type.

    Args:
        polars_dtype: Polars data type

    Returns:
        SQLAlchemy type
    """
    # Handle List types (convert to PostgreSQL ARRAY)
    if isinstance(polars_dtype, pl.datatypes.List):
        inner_type = polars_dtype.inner
        inner_map = {
            pl.String: Text,
            pl.Utf8: Text,
            pl.Int64: Integer,
            pl.Int32: Integer,
            pl.Float64: Float,
            pl.Float32: Float,
        }
        return ARRAY(inner_map.get(inner_type, Text))

    # Handle scalar types
    type_map = {
        pl.Int64: Integer,
        pl.Int32: Integer,
        pl.Float64: Float,
        pl.Float32: Float,
        pl.String: Text,
        pl.Utf8: Text,
        pl.Boolean: Boolean,
        pl.Datetime: DateTime,
        pl.Object: Text,
    }
    return type_map.get(polars_dtype, Text)


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
        sqlalchemy_type = _polars_to_sqlalchemy_type(dtype)
        # All columns are nullable by default
        columns.append(Column(col_name, sqlalchemy_type, nullable=True))

    return Table(table_name, metadata, *columns)


def _prepare_dataframe_for_insert(df: pl.DataFrame) -> pl.DataFrame:
    """Prepare DataFrame for database insertion.

    Converts Python object types (like Polars DataTypeClass) to strings.
    PostgreSQL ARRAY types for List columns are handled natively.

    Args:
        df: Polars DataFrame

    Returns:
        Prepared DataFrame
    """
    result_df = df.clone()

    # Convert Object columns to strings
    for col_name, dtype in zip(df.columns, df.dtypes):
        if dtype == pl.Object:
            result_df = result_df.with_columns(
                pl.col(col_name)
                .map_elements(
                    lambda x: str(x) if x is not None else None,
                    return_dtype=pl.String,
                )
                .alias(col_name)
            )

    return result_df


def save_to_database(
    results_df: pl.DataFrame,
    metadata_df: pl.DataFrame,
    table_prefix: str = "survey",
    append: bool = True,
):
    """Save survey dataframes to PostgreSQL database with native types.

    Creates tables with explicit PostgreSQL types including ARRAY for list columns.
    Uses psycopg2 for efficient bulk insert.

    Args:
        results_df: Survey results DataFrame
        metadata_df: Variable-level metadata DataFrame with question info
        table_prefix: Prefix for table names
        append: If True, append to existing tables; if False, create new tables (fails if exist)
    """
    connection_uri = get_connection_uri()
    engine = create_engine(connection_uri)
    db_metadata = MetaData()

    # Prepare dataframes
    results_prepared = _prepare_dataframe_for_insert(results_df)
    metadata_prepared = _prepare_dataframe_for_insert(metadata_df)

    # Define table names
    results_table_name = f"{table_prefix}_results"
    metadata_table_name = f"{table_prefix}_metadata"

    # Create table schemas from DataFrames
    results_table = _create_table_from_dataframe(
        results_prepared, results_table_name, db_metadata
    )
    metadata_table = _create_table_from_dataframe(
        metadata_prepared, metadata_table_name, db_metadata
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
        if len(results_prepared) > 0:
            results_records = results_prepared.to_dicts()
            # Convert any remaining dict values to strings (shouldn't happen but safety check)
            for record in results_records:
                for key, value in list(record.items()):
                    if isinstance(value, dict):
                        record[key] = str(value)
            conn.execute(results_table.insert(), results_records)

        if len(metadata_prepared) > 0:
            metadata_records = metadata_prepared.to_dicts()
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


def query_database(sql: str) -> pl.DataFrame:
    """Execute SQL query and return results as Polars DataFrame.

    Uses connectorx for efficient reading. PostgreSQL ARRAY types are automatically
    converted to Polars List types.

    Args:
        sql: SQL query to execute

    Returns:
        Query results as Polars DataFrame
    """
    connection_uri = get_connection_uri()

    # Use read_database_uri with connectorx (requires postgresql:// format)
    # Convert from postgresql+psycopg2:// to postgresql://
    cx_uri = connection_uri.replace("postgresql+psycopg2://", "postgresql://")

    result_df = pl.read_database_uri(sql, cx_uri)

    return result_df
