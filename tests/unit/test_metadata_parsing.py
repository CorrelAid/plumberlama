from plumberlama.states import ParsedMetadataState
from plumberlama.transitions import parse_poll_metadata


def test_extract_poll_metadata(sample_loaded_metadata):
    """Test parsing using sample data (no API call).

    This test verifies the parsing logic works correctly without
    needing real API credentials. It uses the sample_loaded_metadata
    fixture which contains preprocessed and validated Questions objects.
    """
    # Parse to DataFrames
    result = parse_poll_metadata(sample_loaded_metadata)

    # Verify state structure
    assert isinstance(result, ParsedMetadataState)
    assert len(result.parsed_metadata_df) > 0, "Should have at least one variable"

    # Verify metadata has question_text column
    assert (
        "question_text" in result.parsed_metadata_df.columns
    ), "Should have question_text column"

    # Verify all variables have non-null question_text
    assert (
        result.parsed_metadata_df["question_text"].null_count() == 0
    ), "All variables must have question text"

    # Verify we have the expected number of unique questions from sample data
    assert (
        result.parsed_metadata_df["question_id"].n_unique() == 8
    ), "Sample data has 8 questions"
