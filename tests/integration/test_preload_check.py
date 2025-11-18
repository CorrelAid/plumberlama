"""Integration tests for preload check and metadata validation.

Tests the preload_check function which checks if existing database
tables match the current survey structure before loading new data.
"""

import pytest

from plumberlama.io.database import save_to_database
from plumberlama.states import PreloadCheckState
from plumberlama.transitions import preload_check


def test_preload_check_no_existing_tables(
    test_db_config, sample_parsed_metadata, db_connection
):
    """Test preload check when no tables exist (first load)."""
    # Ensure tables don't exist by using a unique survey ID
    test_db_config.survey_id = "test_preload_first_load"

    result = preload_check(test_db_config, sample_parsed_metadata)

    assert isinstance(result, PreloadCheckState)
    assert result.load_counter == 0


def test_preload_check_matching_metadata(
    test_db_config,
    sample_parsed_metadata,
    sample_processed_metadata,
    sample_anonymized_results,
    db_connection,
):
    """Test preload check when existing metadata matches current metadata."""
    test_db_config.survey_id = "test_preload_matching"

    # Save initial data to database (with processed metadata from first load)
    save_to_database(
        distributions_df=sample_anonymized_results.distributions_df,
        categorical_df=sample_anonymized_results.categorical_df,
        metadata_df=sample_processed_metadata.final_metadata_df,
        table_prefix=test_db_config.survey_id,
        append=False,
        config=test_db_config,
    )

    # Validate metadata matches (using parsed metadata, simulating second load)
    result = preload_check(test_db_config, sample_parsed_metadata)

    assert isinstance(result, PreloadCheckState)
    assert result.load_counter > 0


def test_preload_check_mismatched_variable_count(
    test_db_config,
    sample_parsed_metadata,
    sample_processed_metadata,
    sample_anonymized_results,
    db_connection,
):
    """Test preload check fails when variable count differs."""
    test_db_config.survey_id = "test_preload_count_mismatch"

    # Save original processed metadata (from first load)
    save_to_database(
        distributions_df=sample_anonymized_results.distributions_df,
        categorical_df=sample_anonymized_results.categorical_df,
        metadata_df=sample_processed_metadata.final_metadata_df,
        table_prefix=test_db_config.survey_id,
        append=False,
        config=test_db_config,
    )

    # Try to validate with fewer variables (simulating second load with different structure)
    from plumberlama.states import ParsedMetadataState

    modified_metadata = ParsedMetadataState(
        parsed_metadata_df=sample_parsed_metadata.parsed_metadata_df.head(10),
    )

    from plumberlama.transitions import MetadataMismatchError

    with pytest.raises(MetadataMismatchError, match="Metadata schema mismatch"):
        preload_check(test_db_config, modified_metadata)


def test_preload_check_mismatched_variable_ids(
    test_db_config,
    sample_parsed_metadata,
    sample_processed_metadata,
    sample_anonymized_results,
    db_connection,
):
    """Test preload check fails when variable IDs differ."""
    import polars as pl

    test_db_config.survey_id = "test_preload_id_mismatch"

    # Save original processed metadata (from first load)
    save_to_database(
        distributions_df=sample_anonymized_results.distributions_df,
        categorical_df=sample_anonymized_results.categorical_df,
        metadata_df=sample_processed_metadata.final_metadata_df,
        table_prefix=test_db_config.survey_id,
        append=False,
        config=test_db_config,
    )

    # Modify parsed metadata by changing one variable ID (simulating different survey structure)
    from plumberlama.states import ParsedMetadataState

    modified_df = sample_parsed_metadata.parsed_metadata_df.clone()
    first_id = modified_df["id"][0]

    modified_df = modified_df.with_columns(
        pl.when(pl.col("id") == first_id)
        .then(pl.lit("CHANGED_ID"))
        .otherwise(pl.col("id"))
        .alias("id")
    )

    modified_metadata = ParsedMetadataState(parsed_metadata_df=modified_df)

    from plumberlama.transitions import MetadataMismatchError

    with pytest.raises(MetadataMismatchError, match="Metadata schema mismatch"):
        preload_check(test_db_config, modified_metadata)


def test_preload_check_mismatched_question_types(
    test_db_config,
    sample_parsed_metadata,
    sample_processed_metadata,
    sample_anonymized_results,
    db_connection,
):
    """Test preload check fails when question types differ."""
    import polars as pl

    test_db_config.survey_id = "test_preload_type_mismatch"

    # Save original processed metadata (from first load)
    save_to_database(
        distributions_df=sample_anonymized_results.distributions_df,
        categorical_df=sample_anonymized_results.categorical_df,
        metadata_df=sample_processed_metadata.final_metadata_df,
        table_prefix=test_db_config.survey_id,
        append=False,
        config=test_db_config,
    )

    # Modify parsed metadata by changing a question type (simulating different survey structure)
    from plumberlama.states import ParsedMetadataState

    modified_df = sample_parsed_metadata.parsed_metadata_df.clone()

    modified_df = modified_df.with_columns(
        pl.when(pl.col("question_type") == "input_single_singleline")
        .then(pl.lit("input_single_integer"))
        .otherwise(pl.col("question_type"))
        .alias("question_type")
    )

    modified_metadata = ParsedMetadataState(parsed_metadata_df=modified_df)

    from plumberlama.transitions import MetadataMismatchError

    with pytest.raises(MetadataMismatchError, match="Metadata schema mismatch"):
        preload_check(test_db_config, modified_metadata)


def test_preload_check_renamed_variables_allowed(
    test_db_config,
    sample_parsed_metadata,
    sample_processed_metadata,
    sample_anonymized_results,
    db_connection,
):
    """Test that preload check allows the same survey structure.

    Since we now compare parsed metadata (id) with database metadata (original_id),
    the comparison checks the raw survey structure (before LLM renaming).
    """
    test_db_config.survey_id = "test_preload_rename_allowed"

    # Save original processed metadata (from first load)
    save_to_database(
        distributions_df=sample_anonymized_results.distributions_df,
        categorical_df=sample_anonymized_results.categorical_df,
        metadata_df=sample_processed_metadata.final_metadata_df,
        table_prefix=test_db_config.survey_id,
        append=False,
        config=test_db_config,
    )

    # Second load with same parsed metadata (simulates LLM might generate different names)
    # This should pass because we compare parsed.id with database.original_id
    result = preload_check(test_db_config, sample_parsed_metadata)
    assert isinstance(result, PreloadCheckState)
