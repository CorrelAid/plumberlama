import pandera.polars as pa

from plumberlama.states import ProcessedMetadataState
from plumberlama.transitions import process_poll_metadata


def test_process_poll_metadata(sample_parsed_metadata, real_config):
    """Test processing metadata with LLM variable naming."""
    result = process_poll_metadata(sample_parsed_metadata, real_config)

    assert isinstance(result, ProcessedMetadataState)
    assert len(result.final_metadata_df) > 0
    assert isinstance(result.processed_results_schema, pa.DataFrameSchema)

    assert "id" in result.final_metadata_df.columns
    assert "original_id" in result.final_metadata_df.columns
    assert "question_id" in result.final_metadata_df.columns
    assert "question_type" in result.final_metadata_df.columns

    assert len(result.processed_results_schema.columns) > 0

    for var_id in result.final_metadata_df["id"]:
        assert var_id in result.processed_results_schema.columns

    question_types = result.final_metadata_df["question_type"].unique()
    for qtype in ["single_choice", "scale", "matrix"]:
        if qtype in question_types:
            vars_of_type = result.final_metadata_df.filter(
                result.final_metadata_df["question_type"] == qtype
            )
            for var_id in vars_of_type["id"]:
                col_schema = result.processed_results_schema.columns[var_id]
                assert col_schema.checks is not None or col_schema.dtype is not None
