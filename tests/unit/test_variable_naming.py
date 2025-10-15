"""Tests for variable naming transformation."""

import polars as pl

from plumberlama.transform.variable_naming import rename_vars_with_labels


def test_single_variable_renamed_simply(single_variable_subset, llm_and_generator):
    """Questions with single variable should be renamed to Q<position> format"""
    metadata_df = single_variable_subset
    llm, generator = llm_and_generator

    updated_metadata_df = rename_vars_with_labels(metadata_df, generator, llm)

    assert "Q1" in updated_metadata_df["id"].to_list()
    assert "V1" not in updated_metadata_df["id"].to_list()
    assert "original_id" in updated_metadata_df.columns
    assert "V1" in updated_metadata_df["original_id"].to_list()
    # Check mapping via original_id column
    v1_row = updated_metadata_df.filter(pl.col("original_id") == "V1")
    assert v1_row["id"][0] == "Q1"


def test_multiple_variables_renamed_with_labels(
    multiple_choice_subset, llm_and_generator
):
    """Questions with multiple variables should be renamed with labels"""
    metadata_df = multiple_choice_subset
    llm, generator = llm_and_generator

    updated_metadata_df = rename_vars_with_labels(metadata_df, generator, llm)

    # Should be renamed with LLM-generated names
    # Check that variables were renamed (exact names depend on LLM output)
    var_ids = updated_metadata_df["id"].to_list()
    assert "V6" not in var_ids
    assert "V7" not in var_ids
    assert "V8" not in var_ids
    # Check that all variables start with Q5_ (question 5 in absolute position)
    assert all(var_id.startswith("Q5_") for var_id in var_ids)
    assert len(var_ids) == 3
    # Check original_id column has the mapping
    assert "original_id" in updated_metadata_df.columns
    original_ids = updated_metadata_df["original_id"].to_list()
    assert "V6" in original_ids
    assert "V7" in original_ids
    assert "V8" in original_ids


def test_multiple_choice_other_suffix_naming(
    multiple_choice_other_subset, llm_and_generator
):
    """Test that 'other' variables get correct suffixes: _other and _other_text"""
    metadata_df = multiple_choice_other_subset
    llm, generator = llm_and_generator

    updated_metadata_df = rename_vars_with_labels(metadata_df, generator, llm)

    # Check that 'other' boolean variable ends with _other
    var_ids = updated_metadata_df["id"].to_list()
    other_boolean_ids = [
        var_id
        for var_id in var_ids
        if var_id.endswith("_other") and not var_id.endswith("_other_text")
    ]
    assert (
        len(other_boolean_ids) == 1
    ), f"Expected exactly one _other variable, found: {other_boolean_ids}"

    # Check that 'other' text variable ends with _other_text
    other_text_ids = [var_id for var_id in var_ids if var_id.endswith("_other_text")]
    assert (
        len(other_text_ids) == 1
    ), f"Expected exactly one _other_text variable, found: {other_text_ids}"

    # Both should start with Q6_ (question 6 in absolute position)
    assert other_boolean_ids[0].startswith("Q6_")
    assert other_text_ids[0].startswith("Q6_")

    # Other non-'other' variables should not have these suffixes
    other_ids = [
        var_id
        for var_id in var_ids
        if not var_id.endswith("_other") and not var_id.endswith("_other_text")
    ]
    assert len(other_ids) == 2  # V42 and V43 (non-other choices)

    # Check original_id column exists
    assert "original_id" in updated_metadata_df.columns
