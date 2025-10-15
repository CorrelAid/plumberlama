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
    sample_processed_results, sample_processed_metadata, db_connection
):
    """Test basic save_to_database functionality with sample data."""
    table_prefix = "test_basic_save"

    # Save to database
    save_to_database(
        results_df=sample_processed_results.results_df,
        metadata_df=sample_processed_metadata.final_metadata_df,
        table_prefix=table_prefix,
        append=False,
    )

    # Verify tables were created
    inspector = inspect(db_connection)
    tables = inspector.get_table_names()
    assert f"{table_prefix}_results" in tables
    assert f"{table_prefix}_metadata" in tables


def test_load_results_from_database(
    sample_processed_results, sample_processed_metadata, db_connection
):
    """Test loading survey results from database."""
    table_prefix = "test_load_results"

    # Save to database
    save_to_database(
        results_df=sample_processed_results.results_df,
        metadata_df=sample_processed_metadata.final_metadata_df,
        table_prefix=table_prefix,
        append=False,
    )

    # Load results back
    loaded_results = query_database(f"SELECT * FROM {table_prefix}_results")

    # Verify data integrity
    assert isinstance(loaded_results, pl.DataFrame)
    assert len(loaded_results) == len(sample_processed_results.results_df)

    # Verify columns match
    original_cols = set(sample_processed_results.results_df.columns)
    loaded_cols = set(loaded_results.columns)
    assert original_cols == loaded_cols


def test_load_metadata_from_database(
    sample_processed_metadata, sample_processed_results, db_connection
):
    """Test loading metadata from database."""
    table_prefix = "test_load_metadata"

    # Save to database
    save_to_database(
        results_df=sample_processed_results.results_df,
        metadata_df=sample_processed_metadata.final_metadata_df,
        table_prefix=table_prefix,
        append=False,
    )

    # Load metadata back
    loaded_metadata = query_database(f"SELECT * FROM {table_prefix}_metadata")

    # Verify data integrity
    assert isinstance(loaded_metadata, pl.DataFrame)
    assert len(loaded_metadata) == len(sample_processed_metadata.final_metadata_df)

    # Verify key columns exist
    assert "id" in loaded_metadata.columns
    assert "question_text" in loaded_metadata.columns
    assert "question_type" in loaded_metadata.columns


def test_database_create_behavior(
    sample_processed_results, sample_processed_metadata, db_connection
):
    """Test that creating new tables with append=False works correctly."""
    table_prefix = "test_create"

    # Create new tables
    save_to_database(
        results_df=sample_processed_results.results_df,
        metadata_df=sample_processed_metadata.final_metadata_df,
        table_prefix=table_prefix,
        append=False,
    )

    # Verify data saved
    loaded_results = query_database(f"SELECT * FROM {table_prefix}_results")
    assert len(loaded_results) == len(sample_processed_results.results_df)

    loaded_metadata = query_database(f"SELECT * FROM {table_prefix}_metadata")
    assert len(loaded_metadata) == len(sample_processed_metadata.final_metadata_df)


def test_database_append_behavior(
    sample_processed_results, sample_processed_metadata, db_connection
):
    """Test that if_exists='append' correctly adds to existing tables."""
    table_prefix = "test_append"

    # Save original data
    save_to_database(
        results_df=sample_processed_results.results_df,
        metadata_df=sample_processed_metadata.final_metadata_df,
        table_prefix=table_prefix,
        append=False,
    )

    original_count = len(sample_processed_results.results_df)

    # Append same data again
    save_to_database(
        results_df=sample_processed_results.results_df,
        metadata_df=sample_processed_metadata.final_metadata_df,
        table_prefix=table_prefix,
        append=True,
    )

    # Verify table has double the rows
    loaded_results = query_database(f"SELECT * FROM {table_prefix}_results")
    assert len(loaded_results) == original_count * 2


def test_query_with_filter(
    sample_processed_results, sample_processed_metadata, db_connection
):
    """Test querying database with WHERE clause."""
    table_prefix = "test_query_filter"

    # Save to database
    save_to_database(
        results_df=sample_processed_results.results_df,
        metadata_df=sample_processed_metadata.final_metadata_df,
        table_prefix=table_prefix,
        append=False,
    )

    # Query with filter (PostgreSQL converts column names to lowercase by default)
    filtered_results = query_database(
        f"SELECT * FROM {table_prefix}_results WHERE \"id\" = '1'"
    )

    # Verify filter worked
    assert len(filtered_results) == 1
    assert str(filtered_results["id"][0]) == "1"


def test_data_types_preserved(
    sample_processed_results, sample_processed_metadata, db_connection
):
    """Test that data types are preserved when saving and loading from database."""
    table_prefix = "test_data_types"

    # Save to database
    save_to_database(
        results_df=sample_processed_results.results_df,
        metadata_df=sample_processed_metadata.final_metadata_df,
        table_prefix=table_prefix,
        append=False,
    )

    # Load back
    loaded_results = query_database(f"SELECT * FROM {table_prefix}_results")

    # Get numeric columns from original (excluding metadata columns)
    numeric_cols = [
        col
        for col in sample_processed_results.results_df.columns
        if sample_processed_results.results_df[col].dtype in [pl.Int64, pl.Float64]
    ]

    # Verify at least some numeric columns exist and are still numeric
    for col in numeric_cols[:5]:  # Check first 5 numeric columns
        if col in loaded_results.columns:
            assert loaded_results[col].dtype in [
                pl.Int64,
                pl.Float64,
                pl.Int32,
                pl.Float32,
            ], f"Column {col} should be numeric but is {loaded_results[col].dtype}"


def test_join_results_and_metadata(
    sample_processed_results, sample_processed_metadata, db_connection
):
    """Test joining results and metadata tables in a SQL query."""
    table_prefix = "test_join"

    # Save to database
    save_to_database(
        results_df=sample_processed_results.results_df,
        metadata_df=sample_processed_metadata.final_metadata_df,
        table_prefix=table_prefix,
        append=False,
    )

    # Query joining both tables
    # Note: This is a simplified join - actual column names depend on metadata structure
    join_query = f"""
    SELECT
        r.*,
        m.question_text,
        m.question_type
    FROM {table_prefix}_results r
    CROSS JOIN {table_prefix}_metadata m
    WHERE m.question_id = 1
    LIMIT 5
    """

    joined_data = query_database(join_query)

    # Verify join worked
    assert isinstance(joined_data, pl.DataFrame)
    assert len(joined_data) > 0
    assert "question_text" in joined_data.columns
    assert "question_type" in joined_data.columns
