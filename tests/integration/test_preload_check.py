"""Integration tests for preload check and metadata validation.

Tests the preload_check function which checks if existing database
tables match the current survey structure before loading new data.
"""

import pytest

from plumberlama.io.database import save_to_database
from plumberlama.states import PreloadCheckState
from plumberlama.transitions import preload_check


def test_preload_check_no_existing_tables(
    test_db_config, sample_processed_metadata, db_connection
):
    """Test preload check when no tables exist (first load)."""
    # Ensure tables don't exist by using a unique survey ID
    test_db_config.survey_id = "test_preload_first_load"

    result = preload_check(test_db_config, sample_processed_metadata)

    assert isinstance(result, PreloadCheckState)
    assert result.load_counter == 0


def test_preload_check_matching_metadata(
    test_db_config, sample_processed_metadata, sample_processed_results, db_connection
):
    """Test preload check when existing metadata matches current metadata."""
    import polars as pl

    test_db_config.survey_id = "test_preload_matching"

    # Add load_counter column (this is normally added by load_data transition)
    results_with_counter = sample_processed_results.results_df.with_columns(
        pl.lit(0).alias("load_counter")
    )

    # Save initial data to database
    save_to_database(
        results_df=results_with_counter,
        metadata_df=sample_processed_metadata.final_metadata_df,
        table_prefix=test_db_config.survey_id,
        append=False,
        config=test_db_config,
    )

    # Validate metadata matches
    result = preload_check(test_db_config, sample_processed_metadata)

    assert isinstance(result, PreloadCheckState)
    assert result.load_counter > 0


def test_preload_check_mismatched_variable_count(
    test_db_config, sample_processed_metadata, db_connection
):
    """Test preload check fails when variable count differs."""
    test_db_config.survey_id = "test_preload_count_mismatch"

    # Save original metadata
    save_to_database(
        results_df=sample_processed_metadata.final_metadata_df.head(
            1
        ),  # Dummy results df
        metadata_df=sample_processed_metadata.final_metadata_df,
        table_prefix=test_db_config.survey_id,
        append=False,
        config=test_db_config,
    )

    # Try to validate with fewer variables
    # Create new state with modified metadata
    from plumberlama.states import ProcessedMetadataState

    modified_metadata = ProcessedMetadataState(
        final_metadata_df=sample_processed_metadata.final_metadata_df.head(10),
        processed_results_schema=sample_processed_metadata.processed_results_schema,
    )

    from plumberlama.transitions import MetadataMismatchError

    with pytest.raises(MetadataMismatchError, match="Metadata schema mismatch"):
        preload_check(test_db_config, modified_metadata)


def test_preload_check_mismatched_variable_ids(
    test_db_config, sample_processed_metadata, db_connection
):
    """Test preload check fails when variable IDs differ."""
    import polars as pl

    test_db_config.survey_id = "test_preload_id_mismatch"

    # Save original metadata
    save_to_database(
        results_df=sample_processed_metadata.final_metadata_df.head(
            1
        ),  # Dummy results df
        metadata_df=sample_processed_metadata.final_metadata_df,
        table_prefix=test_db_config.survey_id,
        append=False,
        config=test_db_config,
    )

    # Modify metadata by changing one original_id
    from plumberlama.states import ProcessedMetadataState

    modified_df = sample_processed_metadata.final_metadata_df.clone()
    first_original_id = modified_df["original_id"][0]

    modified_df = modified_df.with_columns(
        pl.when(pl.col("original_id") == first_original_id)
        .then(pl.lit("CHANGED_ID"))
        .otherwise(pl.col("original_id"))
        .alias("original_id")
    )

    modified_metadata = ProcessedMetadataState(
        final_metadata_df=modified_df,
        processed_results_schema=sample_processed_metadata.processed_results_schema,
    )

    from plumberlama.transitions import MetadataMismatchError

    with pytest.raises(MetadataMismatchError, match="Metadata schema mismatch"):
        preload_check(test_db_config, modified_metadata)


def test_preload_check_mismatched_question_types(
    test_db_config, sample_processed_metadata, db_connection
):
    """Test preload check fails when question types differ."""
    import polars as pl

    test_db_config.survey_id = "test_preload_type_mismatch"

    # Save original metadata
    save_to_database(
        results_df=sample_processed_metadata.final_metadata_df.head(
            1
        ),  # Dummy results df
        metadata_df=sample_processed_metadata.final_metadata_df,
        table_prefix=test_db_config.survey_id,
        append=False,
        config=test_db_config,
    )

    # Modify metadata by changing a question type
    from plumberlama.states import ProcessedMetadataState

    modified_df = sample_processed_metadata.final_metadata_df.clone()

    modified_df = modified_df.with_columns(
        pl.when(pl.col("question_type") == "input_single_singleline")
        .then(pl.lit("input_single_integer"))
        .otherwise(pl.col("question_type"))
        .alias("question_type")
    )

    modified_metadata = ProcessedMetadataState(
        final_metadata_df=modified_df,
        processed_results_schema=sample_processed_metadata.processed_results_schema,
    )

    from plumberlama.transitions import MetadataMismatchError

    with pytest.raises(MetadataMismatchError, match="Metadata schema mismatch"):
        preload_check(test_db_config, modified_metadata)


def test_preload_check_renamed_variables_allowed(
    test_db_config, sample_processed_metadata, db_connection
):
    """Test that preload check allows changes to renamed variable IDs.

    The 'id' column (renamed variables) can change between loads,
    only original_id and question_type must match.
    """
    import polars as pl

    test_db_config.survey_id = "test_preload_rename_allowed"

    # Create dummy results with load_counter
    dummy_results = sample_processed_metadata.final_metadata_df.head(1).with_columns(
        pl.lit(0).alias("load_counter")
    )

    # Save original metadata
    save_to_database(
        results_df=dummy_results,
        metadata_df=sample_processed_metadata.final_metadata_df,
        table_prefix=test_db_config.survey_id,
        append=False,
        config=test_db_config,
    )

    # Modify only the renamed 'id' column (should still pass)
    from plumberlama.states import ProcessedMetadataState

    modified_df = sample_processed_metadata.final_metadata_df.clone()
    first_id = modified_df["id"][0]

    modified_df = modified_df.with_columns(
        pl.when(pl.col("id") == first_id)
        .then(pl.lit("new_renamed_id"))
        .otherwise(pl.col("id"))
        .alias("id")
    )

    modified_metadata = ProcessedMetadataState(
        final_metadata_df=modified_df,
        processed_results_schema=sample_processed_metadata.processed_results_schema,
    )

    # This should pass because we only check original_id and question_type
    result = preload_check(test_db_config, modified_metadata)
    assert isinstance(result, PreloadCheckState)
