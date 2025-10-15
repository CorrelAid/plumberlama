import polars as pl
import pytest
from polars.testing import assert_frame_equal


def test_frame_equal_with_identical_metadata(sample_processed_metadata):
    """Test that identical metadata passes assert_frame_equal."""
    df1 = sample_processed_metadata.final_metadata_df.sort("original_id").select(
        ["original_id", "id", "question_type"]
    )
    df2 = sample_processed_metadata.final_metadata_df.sort("original_id").select(
        ["original_id", "id", "question_type"]
    )

    # Should not raise
    assert_frame_equal(df1, df2, check_row_order=True, check_column_order=False)


def test_frame_equal_fails_on_different_variable_count(sample_processed_metadata):
    """Test that validation fails when number of variables differs."""
    df1 = sample_processed_metadata.final_metadata_df.sort("original_id").select(
        ["original_id", "id", "question_type"]
    )
    df2 = (
        sample_processed_metadata.final_metadata_df.head(5)
        .sort("original_id")
        .select(["original_id", "id", "question_type"])
    )

    with pytest.raises(AssertionError):
        assert_frame_equal(df1, df2, check_row_order=True, check_column_order=False)


def test_frame_equal_fails_on_different_variable_ids(sample_processed_metadata):
    """Test that validation fails when variable IDs differ."""
    modified_df = sample_processed_metadata.final_metadata_df.clone()
    modified_df = modified_df.with_columns(
        pl.when(pl.col("original_id") == modified_df["original_id"][0])
        .then(pl.lit("CHANGED_ID"))
        .otherwise(pl.col("original_id"))
        .alias("original_id")
    )

    df1 = sample_processed_metadata.final_metadata_df.sort("original_id").select(
        ["original_id", "id", "question_type"]
    )
    df2 = modified_df.sort("original_id").select(["original_id", "id", "question_type"])

    with pytest.raises(AssertionError):
        assert_frame_equal(df1, df2, check_row_order=True, check_column_order=False)


def test_frame_equal_fails_on_different_renamed_ids(sample_processed_metadata):
    """Test that validation fails when renamed variable IDs differ."""
    modified_df = sample_processed_metadata.final_metadata_df.clone()
    first_id = modified_df["id"][0]

    modified_df = modified_df.with_columns(
        pl.when(pl.col("id") == first_id)
        .then(pl.lit("CHANGED_RENAMED_ID"))
        .otherwise(pl.col("id"))
        .alias("id")
    )

    df1 = sample_processed_metadata.final_metadata_df.sort("original_id").select(
        ["original_id", "id", "question_type"]
    )
    df2 = modified_df.sort("original_id").select(["original_id", "id", "question_type"])

    with pytest.raises(AssertionError):
        assert_frame_equal(df1, df2, check_row_order=True, check_column_order=False)


def test_frame_equal_fails_on_different_question_types(sample_processed_metadata):
    """Test that validation fails when question types differ."""
    modified_df = sample_processed_metadata.final_metadata_df.clone()

    modified_df = modified_df.with_columns(
        pl.when(pl.col("question_type") == "single_choice")
        .then(pl.lit("multiple_choice"))
        .otherwise(pl.col("question_type"))
        .alias("question_type")
    )

    df1 = sample_processed_metadata.final_metadata_df.sort("original_id").select(
        ["original_id", "id", "question_type"]
    )
    df2 = modified_df.sort("original_id").select(["original_id", "id", "question_type"])

    with pytest.raises(AssertionError):
        assert_frame_equal(df1, df2, check_row_order=True, check_column_order=False)
