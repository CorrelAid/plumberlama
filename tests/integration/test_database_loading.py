import polars as pl
from sqlalchemy import inspect, text

from plumberlama.io.database import query_database, save_to_database


def test_database_connection(db_connection):
    """Test that database connection is working."""
    with db_connection.connect() as conn:
        result = conn.execute(text("SELECT version()"))
        version = result.fetchone()[0]
        assert "PostgreSQL" in version


def test_save_to_database_basic(
    sample_anonymized_results, sample_processed_metadata, test_db_config, db_connection
):
    """Test basic save_to_database functionality with anonymized data."""
    table_prefix = "test_basic_save"

    # Save to database (with anonymized data)
    save_to_database(
        distributions_df=sample_anonymized_results.distributions_df,
        categorical_df=sample_anonymized_results.categorical_df,
        metadata_df=sample_processed_metadata.final_metadata_df,
        table_prefix=table_prefix,
        append=False,
        config=test_db_config,
    )

    # Verify tables were created
    inspector = inspect(db_connection)
    tables = inspector.get_table_names()
    assert f"{table_prefix}_distributions" in tables
    assert f"{table_prefix}_categorical" in tables
    assert f"{table_prefix}_metadata" in tables


def test_load_distributions_from_database(
    sample_anonymized_results, sample_processed_metadata, test_db_config, db_connection
):
    """Test loading distributions from database."""
    table_prefix = "test_load_distributions"

    # Save to database
    save_to_database(
        distributions_df=sample_anonymized_results.distributions_df,
        categorical_df=sample_anonymized_results.categorical_df,
        metadata_df=sample_processed_metadata.final_metadata_df,
        table_prefix=table_prefix,
        append=False,
        config=test_db_config,
    )

    # Load distributions back
    loaded_distributions = query_database(
        f"SELECT * FROM {table_prefix}_distributions", config=test_db_config
    )

    # Verify data integrity
    assert isinstance(loaded_distributions, pl.DataFrame)
    assert len(loaded_distributions) > 0
    assert "variable_id" in loaded_distributions.columns
    assert "value" in loaded_distributions.columns
    assert "load_counter" in loaded_distributions.columns


def test_load_categorical_from_database(
    sample_anonymized_results, sample_processed_metadata, test_db_config, db_connection
):
    """Test loading categorical data from database."""
    table_prefix = "test_load_categorical"

    # Save to database
    save_to_database(
        distributions_df=sample_anonymized_results.distributions_df,
        categorical_df=sample_anonymized_results.categorical_df,
        metadata_df=sample_processed_metadata.final_metadata_df,
        table_prefix=table_prefix,
        append=False,
        config=test_db_config,
    )

    # Load categorical back
    loaded_categorical = query_database(
        f"SELECT * FROM {table_prefix}_categorical", config=test_db_config
    )

    # Verify data integrity
    assert isinstance(loaded_categorical, pl.DataFrame)
    assert len(loaded_categorical) > 0
    assert "variable_id" in loaded_categorical.columns
    assert "value" in loaded_categorical.columns
    assert "count" in loaded_categorical.columns
    assert "load_counter" in loaded_categorical.columns


def test_load_metadata_from_database(
    sample_processed_metadata, sample_anonymized_results, test_db_config, db_connection
):
    """Test loading metadata from database."""
    table_prefix = "test_load_metadata"

    # Save to database
    save_to_database(
        distributions_df=sample_anonymized_results.distributions_df,
        categorical_df=sample_anonymized_results.categorical_df,
        metadata_df=sample_processed_metadata.final_metadata_df,
        table_prefix=table_prefix,
        append=False,
        config=test_db_config,
    )

    # Load metadata back
    loaded_metadata = query_database(
        f"SELECT * FROM {table_prefix}_metadata", config=test_db_config
    )

    # Verify data integrity
    assert isinstance(loaded_metadata, pl.DataFrame)
    assert len(loaded_metadata) == len(sample_processed_metadata.final_metadata_df)

    # Verify key columns exist
    assert "id" in loaded_metadata.columns
    assert "question_text" in loaded_metadata.columns
    assert "question_type" in loaded_metadata.columns
    assert "anonymization_type" in loaded_metadata.columns


def test_database_create_behavior(
    sample_anonymized_results, sample_processed_metadata, test_db_config, db_connection
):
    """Test that creating new tables with append=False works correctly."""
    table_prefix = "test_create"

    # Create new tables
    save_to_database(
        distributions_df=sample_anonymized_results.distributions_df,
        categorical_df=sample_anonymized_results.categorical_df,
        metadata_df=sample_processed_metadata.final_metadata_df,
        table_prefix=table_prefix,
        append=False,
        config=test_db_config,
    )

    # Verify data saved
    loaded_distributions = query_database(
        f"SELECT * FROM {table_prefix}_distributions", config=test_db_config
    )
    assert len(loaded_distributions) > 0

    loaded_categorical = query_database(
        f"SELECT * FROM {table_prefix}_categorical", config=test_db_config
    )
    assert len(loaded_categorical) > 0

    loaded_metadata = query_database(
        f"SELECT * FROM {table_prefix}_metadata", config=test_db_config
    )
    assert len(loaded_metadata) == len(sample_processed_metadata.final_metadata_df)


def test_database_append_behavior(
    sample_anonymized_results, sample_processed_metadata, test_db_config, db_connection
):
    """Test that append=True correctly adds to existing tables."""
    table_prefix = "test_append"

    # Save original data
    save_to_database(
        distributions_df=sample_anonymized_results.distributions_df,
        categorical_df=sample_anonymized_results.categorical_df,
        metadata_df=sample_processed_metadata.final_metadata_df,
        table_prefix=table_prefix,
        append=False,
        config=test_db_config,
    )

    original_dist_count = len(sample_anonymized_results.distributions_df)
    original_cat_count = len(sample_anonymized_results.categorical_df)

    # Append same data again
    save_to_database(
        distributions_df=sample_anonymized_results.distributions_df,
        categorical_df=sample_anonymized_results.categorical_df,
        metadata_df=sample_processed_metadata.final_metadata_df,
        table_prefix=table_prefix,
        append=True,
        config=test_db_config,
    )

    # Verify tables have double the rows
    loaded_distributions = query_database(
        f"SELECT * FROM {table_prefix}_distributions", config=test_db_config
    )
    assert len(loaded_distributions) == original_dist_count * 2

    loaded_categorical = query_database(
        f"SELECT * FROM {table_prefix}_categorical", config=test_db_config
    )
    assert len(loaded_categorical) == original_cat_count * 2

    # Metadata should not have been duplicated (append mode doesn't insert metadata)
    loaded_metadata = query_database(
        f"SELECT * FROM {table_prefix}_metadata", config=test_db_config
    )
    assert len(loaded_metadata) == len(sample_processed_metadata.final_metadata_df)


def test_query_with_filter(
    sample_anonymized_results, sample_processed_metadata, test_db_config, db_connection
):
    """Test querying database with WHERE clause."""
    table_prefix = "test_query_filter"

    # Save to database
    save_to_database(
        distributions_df=sample_anonymized_results.distributions_df,
        categorical_df=sample_anonymized_results.categorical_df,
        metadata_df=sample_processed_metadata.final_metadata_df,
        table_prefix=table_prefix,
        append=False,
        config=test_db_config,
    )

    # Query with filter
    filtered_categorical = query_database(
        f"SELECT * FROM {table_prefix}_categorical WHERE load_counter = 0",
        config=test_db_config,
    )

    # Verify filter worked
    assert len(filtered_categorical) > 0
    assert all(filtered_categorical["load_counter"] == 0)


def test_load_counter_preserved(
    sample_anonymized_results, sample_processed_metadata, test_db_config, db_connection
):
    """Test that load_counter is preserved in database."""
    table_prefix = "test_load_counter"

    # Save to database
    save_to_database(
        distributions_df=sample_anonymized_results.distributions_df,
        categorical_df=sample_anonymized_results.categorical_df,
        metadata_df=sample_processed_metadata.final_metadata_df,
        table_prefix=table_prefix,
        append=False,
        config=test_db_config,
    )

    # Load distributions
    loaded_distributions = query_database(
        f"SELECT * FROM {table_prefix}_distributions", config=test_db_config
    )

    # Verify load_counter exists and is correct
    assert "load_counter" in loaded_distributions.columns
    assert all(loaded_distributions["load_counter"] == 0)

    # Load categorical
    loaded_categorical = query_database(
        f"SELECT * FROM {table_prefix}_categorical", config=test_db_config
    )

    # Verify load_counter exists and is correct
    assert "load_counter" in loaded_categorical.columns
    assert all(loaded_categorical["load_counter"] == 0)
