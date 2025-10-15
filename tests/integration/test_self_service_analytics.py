"""Integration tests for self-service analytics queries.

These tests demonstrate typical analytical queries that end users would run
to analyze survey data and create visualizations. They validate that the
database structure (results + metadata tables) supports real-world analytics
scenarios.
"""

import polars as pl

from plumberlama.io import database_queries
from plumberlama.io.database import query_database, save_to_database


def test_self_service_get_question_metadata(
    sample_processed_results, sample_processed_metadata, db_connection
):
    """Test retrieving metadata for a specific question - typical self-service query."""
    table_prefix = "test_self_service_meta"

    # Save to database
    save_to_database(
        results_df=sample_processed_results.results_df,
        metadata_df=sample_processed_metadata.final_metadata_df,
        table_prefix=table_prefix,
        append=False,
    )

    # Self-service query: Get all metadata for a specific question
    query = database_queries.get_question_metadata(table_prefix, question_id=5)
    metadata = query_database(query)

    # Verify we got metadata for the question
    assert isinstance(metadata, pl.DataFrame)
    assert len(metadata) > 0
    assert "variable_name" in metadata.columns
    assert "variable_label" in metadata.columns
    assert "question_text" in metadata.columns


def test_self_service_frequency_analysis(
    sample_processed_results, sample_processed_metadata, db_connection
):
    """Test frequency analysis with labels - typical self-service analytics."""
    table_prefix = "test_self_service_freq"

    # Save to database with load_counter
    results_with_counter = sample_processed_results.results_df.with_columns(
        pl.lit(0).alias("load_counter")
    )

    save_to_database(
        results_df=results_with_counter,
        metadata_df=sample_processed_metadata.final_metadata_df,
        table_prefix=table_prefix,
        append=False,
    )

    # Self-service query: Get frequency distribution for a choice question
    # Find a single_choice variable first
    metadata = query_database(
        database_queries.find_variable_by_question_type(table_prefix, "single_choice")
    )

    if len(metadata) > 0:
        var_name = metadata["id"][0]

        # Frequency analysis query
        query = database_queries.get_frequency_distribution(table_prefix, var_name)
        freq_results = query_database(query)

        assert isinstance(freq_results, pl.DataFrame)
        assert "count" in freq_results.columns
        assert "percentage" in freq_results.columns


def test_self_service_time_series_analysis(
    sample_processed_results, sample_processed_metadata, db_connection
):
    """Test analyzing data across multiple waves using load_counter."""
    table_prefix = "test_self_service_timeseries"

    # Simulate three waves of data collection
    for wave in range(3):
        results_with_counter = sample_processed_results.results_df.with_columns(
            pl.lit(wave).alias("load_counter")
        )

        save_to_database(
            results_df=results_with_counter,
            metadata_df=sample_processed_metadata.final_metadata_df,
            table_prefix=table_prefix,
            append=(wave > 0),
        )

    # Self-service query: Analyze trends across waves
    # Get a scale variable
    metadata = query_database(
        database_queries.find_variable_by_question_type(table_prefix, "scale")
    )

    if len(metadata) > 0:
        var_name = metadata["id"][0]

        query = database_queries.get_time_series_analysis(table_prefix, var_name)
        timeseries = query_database(query)

        assert isinstance(timeseries, pl.DataFrame)
        assert len(timeseries) == 3  # Three waves
        assert "wave" in timeseries.columns
        assert "avg_value" in timeseries.columns
        assert "response_count" in timeseries.columns


def test_self_service_matrix_question_analysis(
    sample_processed_results, sample_processed_metadata, db_connection
):
    """Test analyzing matrix questions with scale labels for visualization."""
    table_prefix = "test_self_service_matrix"

    results_with_counter = sample_processed_results.results_df.with_columns(
        pl.lit(0).alias("load_counter")
    )

    save_to_database(
        results_df=results_with_counter,
        metadata_df=sample_processed_metadata.final_metadata_df,
        table_prefix=table_prefix,
        append=False,
    )

    # Self-service query: Get matrix question data with labels for charting
    query = database_queries.get_matrix_question_metadata(table_prefix)
    matrix_metadata = query_database(query)

    if len(matrix_metadata) > 0:
        # Verify scale labels are available for visualization
        assert "scale_labels" in matrix_metadata.columns
        assert "item_label" in matrix_metadata.columns

        # Get the actual response data for the first matrix variable
        var_name = matrix_metadata["variable_name"][0]

        results_query = database_queries.get_matrix_question_responses(
            table_prefix, var_name
        )
        matrix_results = query_database(results_query)

        assert isinstance(matrix_results, pl.DataFrame)
        assert "score" in matrix_results.columns
        assert "frequency" in matrix_results.columns
