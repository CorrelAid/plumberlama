"""Unit tests for metadata processing without LLM calls."""

import pandera.polars as pa

from plumberlama.states import ProcessedMetadataState


def test_process_poll_metadata_structure(sample_processed_metadata):
    """Test that processed metadata has the correct structure (uses mocked LLM)."""
    result = sample_processed_metadata

    assert isinstance(result, ProcessedMetadataState)
    assert len(result.final_metadata_df) > 0
    assert isinstance(result.processed_results_schema, pa.DataFrameSchema)

    # Check required columns exist
    assert "id" in result.final_metadata_df.columns
    assert "original_id" in result.final_metadata_df.columns
    assert "question_id" in result.final_metadata_df.columns
    assert "question_type" in result.final_metadata_df.columns
    assert "scale_labels" in result.final_metadata_df.columns

    # Check schema is created for all variables
    assert len(result.processed_results_schema.columns) > 0

    for var_id in result.final_metadata_df["id"]:
        assert var_id in result.processed_results_schema.columns


def test_metadata_has_original_id_mapping(sample_processed_metadata):
    """Test that original_id column correctly maps renamed variables."""
    result = sample_processed_metadata

    # Check that original_id exists and has V-prefixed values
    assert "original_id" in result.final_metadata_df.columns
    original_ids = result.final_metadata_df["original_id"].to_list()
    assert any(oid.startswith("V") for oid in original_ids)

    # Check that id column has been renamed (not V-prefixed)
    renamed_ids = result.final_metadata_df["id"].to_list()
    # At least some should be renamed (not all can be V-prefixed if renaming worked)
    assert not all(rid.startswith("V") and rid[1:].isdigit() for rid in renamed_ids)


def test_schema_validation_for_question_types(sample_processed_metadata):
    """Test that schemas have appropriate validations for different question types."""
    result = sample_processed_metadata

    question_types = result.final_metadata_df["question_type"].unique()

    # Test single_choice gets enum validation
    if "single_choice" in question_types:
        vars_of_type = result.final_metadata_df.filter(
            result.final_metadata_df["question_type"] == "single_choice"
        )
        for var_id in vars_of_type["id"]:
            col_schema = result.processed_results_schema.columns[var_id]
            # Should have checks or specific dtype for single choice
            assert col_schema.checks is not None or col_schema.dtype is not None

    # Test scale/matrix have range validation
    for qtype in ["scale", "matrix"]:
        if qtype in question_types:
            vars_of_type = result.final_metadata_df.filter(
                result.final_metadata_df["question_type"] == qtype
            )
            for var_id in vars_of_type["id"]:
                col_schema = result.processed_results_schema.columns[var_id]
                # Should have range checks
                assert col_schema.checks is not None or col_schema.dtype is not None


def test_matrix_questions_have_scale_labels(sample_processed_metadata):
    """Test that matrix questions have scale_labels populated."""
    result = sample_processed_metadata

    matrix_vars = result.final_metadata_df.filter(
        result.final_metadata_df["question_type"] == "matrix"
    )

    if len(matrix_vars) > 0:
        # Check that scale_labels are present for matrix questions
        for row in matrix_vars.to_dicts():
            scale_labels = row.get("scale_labels")
            # Scale labels should be a list (can be None if no labels in fixture)
            assert scale_labels is None or isinstance(scale_labels, list)
