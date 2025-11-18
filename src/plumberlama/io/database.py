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
    distributions_df: pl.DataFrame,
    categorical_df: pl.DataFrame,
    metadata_df: pl.DataFrame,
    table_prefix: str = "survey",
    append: bool = True,
    config: Optional[Config] = None,
):
    """Save anonymized survey data to PostgreSQL database.

    Args:
        distributions_df: Shuffled individual values (variable_id, value, load_counter)
        categorical_df: Aggregated counts (variable_id, value, count, load_counter)
        metadata_df: Variable metadata
        table_prefix: Prefix for table names
        append: Whether to append or create new tables
        config: Configuration with database connection info

    Returns:
        Success message
    """

    connection_uri = config.get_db_connection_uri()
    engine = create_engine(connection_uri)
    db_metadata = MetaData()

    # Define table names
    distributions_table_name = f"{table_prefix}_distributions"
    categorical_table_name = f"{table_prefix}_categorical"
    metadata_table_name = f"{table_prefix}_metadata"

    with engine.begin() as conn:
        if append:
            # Reflect existing tables from database
            db_metadata.reflect(
                conn,
                only=[
                    distributions_table_name,
                    categorical_table_name,
                    metadata_table_name,
                ],
            )
            distributions_table = db_metadata.tables[distributions_table_name]
            categorical_table = db_metadata.tables[categorical_table_name]
            # metadata_table only needed for verification, not insertion

            # Insert distributions and categorical data
            if len(distributions_df) > 0:
                distributions_records = distributions_df.to_dicts()
                conn.execute(distributions_table.insert(), distributions_records)

            if len(categorical_df) > 0:
                categorical_records = categorical_df.to_dicts()
                conn.execute(categorical_table.insert(), categorical_records)
        else:
            # Create table schemas from DataFrames
            distributions_table = _create_table_from_dataframe(
                distributions_df, distributions_table_name, db_metadata
            )
            categorical_table = _create_table_from_dataframe(
                categorical_df, categorical_table_name, db_metadata
            )
            metadata_table = _create_table_from_dataframe(
                metadata_df, metadata_table_name, db_metadata
            )

            # Create tables, fail if they already exist
            db_metadata.create_all(
                conn,
                tables=[distributions_table, categorical_table, metadata_table],
            )

            # Insert all data for first load
            if len(distributions_df) > 0:
                distributions_records = distributions_df.to_dicts()
                conn.execute(distributions_table.insert(), distributions_records)

            if len(categorical_df) > 0:
                categorical_records = categorical_df.to_dicts()
                conn.execute(categorical_table.insert(), categorical_records)

            metadata_records = metadata_df.to_dicts()
            conn.execute(metadata_table.insert(), metadata_records)

    # Log confirmation
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "survey_data")

    logger.info("✓ Saved to PostgreSQL database:")
    logger.info(f"  - {distributions_table_name}: {len(distributions_df)} rows")
    logger.info(f"  - {categorical_table_name}: {len(categorical_df)} rows")
    logger.info(f"  - {metadata_table_name}: {len(metadata_df)} rows")
    logger.info(f"  - Database location: {db_host}:{db_port}/{db_name}")

    return "success"


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
