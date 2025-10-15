"""Unit tests for variable naming transformation with mocked data."""

import polars as pl


def test_single_variable_questions_renamed(sample_processed_metadata):
    """Test that single variable questions are renamed to semantic names."""
    metadata_df = sample_processed_metadata.final_metadata_df

    # Find a single-variable question
    single_var_questions = (
        metadata_df.group_by("question_id")
        .agg(pl.len().alias("var_count"))
        .filter(pl.col("var_count") == 1)
    )

    if len(single_var_questions) > 0:
        question_id = single_var_questions["question_id"][0]
        var_row = metadata_df.filter(pl.col("question_id") == question_id).row(
            0, named=True
        )

        # Should be renamed (not V-prefixed with just digits)
        var_id = var_row["id"]
        assert not (var_id.startswith("V") and var_id[1:].isdigit())


def test_multiple_variables_have_semantic_names(sample_processed_metadata):
    """Test that multi-variable questions have semantic names."""
    metadata_df = sample_processed_metadata.final_metadata_df

    # Find a multi-variable question
    multi_var_questions = (
        metadata_df.group_by("question_id")
        .agg(pl.len().alias("var_count"))
        .filter(pl.col("var_count") > 1)
    )

    if len(multi_var_questions) > 0:
        question_id = multi_var_questions["question_id"][0]
        vars_df = metadata_df.filter(pl.col("question_id") == question_id)

        # All variables should be renamed (not just V1, V2, V3...)
        for var_id in vars_df["id"]:
            assert not (var_id.startswith("V") and var_id[1:].isdigit())


def test_no_umlauts_in_column_names(sample_processed_metadata):
    """Test that column names do not contain German umlauts or special UTF-8 characters."""
    metadata_df = sample_processed_metadata.final_metadata_df

    # Check all variable IDs for umlauts
    forbidden_chars = ["ä", "ö", "ü", "ß", "Ä", "Ö", "Ü"]

    for var_id in metadata_df["id"]:
        for char in forbidden_chars:
            assert (
                char not in var_id
            ), f"Variable ID '{var_id}' contains forbidden character '{char}'"

        # Also verify only ASCII characters
        assert (
            var_id.encode("ascii", "ignore").decode("ascii") == var_id
        ), f"Variable ID '{var_id}' contains non-ASCII characters"


def test_original_id_preserved(sample_processed_metadata):
    """Test that original_id column preserves original variable names."""
    metadata_df = sample_processed_metadata.final_metadata_df

    assert "original_id" in metadata_df.columns

    # Original IDs should be V-prefixed
    original_ids = metadata_df["original_id"].to_list()
    assert all(
        oid.startswith("V") or oid.startswith("v") for oid in original_ids if oid
    )


def test_multiple_choice_other_has_correct_suffixes(sample_processed_metadata):
    """Test that 'other' variables have _other and _other_text suffixes."""
    metadata_df = sample_processed_metadata.final_metadata_df

    # Find multiple_choice_other questions
    other_questions = metadata_df.filter(
        pl.col("question_type") == "multiple_choice_other"
    )

    if len(other_questions) > 0:
        # Check for _other boolean variable
        other_boolean = other_questions.filter(pl.col("is_other_boolean"))  # noqa: E712
        if len(other_boolean) > 0:
            assert any(var_id.endswith("_other") for var_id in other_boolean["id"])

        # Check for _other_text variable
        other_text = other_questions.filter(pl.col("is_other_text"))  # noqa: E712
        if len(other_text) > 0:
            assert any(var_id.endswith("_other_text") for var_id in other_text["id"])


def test_no_v_number_pattern_in_renamed_ids(sample_processed_metadata):
    """Test that renamed variables don't follow V<number> pattern."""
    metadata_df = sample_processed_metadata.final_metadata_df

    # Check that id column doesn't have V1, V2, V3... pattern
    v_pattern_ids = metadata_df.filter(
        pl.col("id").str.starts_with("V")
        & pl.col("id").str.slice(1, 2).str.contains(r"\d")
    )

    # Should be empty (all should be renamed)
    assert len(v_pattern_ids) == 0


def test_no_duplicate_variable_names(sample_processed_metadata):
    """Test that there are no duplicate variable names after renaming."""
    metadata_df = sample_processed_metadata.final_metadata_df

    duplicates = (
        metadata_df.group_by("id")
        .agg(pl.len().alias("count"))
        .filter(pl.col("count") > 1)
    )

    assert (
        len(duplicates) == 0
    ), f"Found duplicate variable IDs: {duplicates['id'].to_list()}"
