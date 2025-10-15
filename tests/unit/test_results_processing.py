import polars as pl

from plumberlama.states import FetchedResultsState
from plumberlama.transitions import process_poll_results


def test_process_poll_results(sample_processed_metadata, sample_loaded_results):
    """Test processing poll results through the full pipeline."""
    loaded_results = sample_loaded_results

    result = process_poll_results(sample_processed_metadata, loaded_results)
    results_df = result.results_df

    # Basic assertions
    assert results_df is not None
    assert len(results_df) > 0


def test_process_poll_results_with_empty_enum_values(sample_processed_metadata):
    """Test that empty strings in enum columns are handled correctly (converted to None)."""
    # Create mock results with empty strings in a single-choice (enum) column
    # Based on sample_questions_list, V5 is a single choice question (satisfaction)
    mock_results = {
        "vID": ["1", "2", "3"],
        "vCOMPLETED": ["1", "1", "1"],
        "vFINISHED": ["1", "1", "1"],
        "vDURATION": ["120.5", "95.3", "45.2"],
        "vQUOTE": ["Q1", "Q2", "Q3"],
        "vSTART": ["2024-01-01 10:00:00", "2024-01-01 11:00:00", "2024-01-01 12:00:00"],
        "vEND": ["2024-01-01 10:02:00", "2024-01-01 11:01:00", "2024-01-01 12:00:45"],
        "vRUNTIME": ["120s", "95s", "45s"],
        "vPAGETIME1": ["30", "25", "15"],
        "vPAGETIME2": ["50", "40", "20"],
        "vPAGETIME3": ["40", "30", "10"],
        "vDATE": ["2024-01-01", "2024-01-01", "2024-01-01"],
        "vANONYM": ["0", "0", "0"],
        "vLANG": ["de", "de", "de"],
        # Sample responses - V5 is single choice with codes 1-4
        "V1": ["Alice", "Bob", "Charlie"],
        "V2": ["25", "30", "35"],
        "V3": ["alice@test.com", "bob@test.com", "charlie@test.com"],
        "V4": ["123456", "234567", "345678"],
        "V5": ["1", "", "3"],  # Empty string in the middle - this should become None
        "V6": ["1", "0", "1"],
        "V7": ["0", "1", "0"],
        "V8": ["1", "1", "0"],
        "V9": ["1", "0", "1"],
        "V10": ["1", "1", "0"],
        "V11": ["0", "0", "1"],
        "V11.1": ["", "", "Other reason"],
        "V12": ["1", "2", "3"],
        "V13": ["2", "3", "1"],
        "V14": ["3", "2", "2"],
        "V15": ["8", "9", "7"],
    }

    raw_results_df = pl.DataFrame(mock_results)
    fetched_results = FetchedResultsState(raw_results_df=raw_results_df)

    # This should not raise an error
    result = process_poll_results(sample_processed_metadata, fetched_results)
    results_df = result.results_df

    # Check that the empty string was converted to None
    # After renaming, V5 becomes "satisfaction"
    assert "satisfaction" in results_df.columns

    # Get the satisfaction column values
    satisfaction_values = results_df["satisfaction"].to_list()

    # First value should be valid enum, second should be None, third should be valid enum
    assert satisfaction_values[0] is not None  # "1" -> valid enum value
    assert satisfaction_values[1] is None  # "" -> None
    assert satisfaction_values[2] is not None  # "3" -> valid enum value
