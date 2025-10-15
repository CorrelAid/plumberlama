import polars as pl

from plumberlama.states import FetchedResultsState
from plumberlama.transitions import fetch_poll_results


def test_load_poll_results_success(sample_processed_metadata, real_config):
    result = fetch_poll_results(real_config)

    # Verify state structure
    assert isinstance(result, FetchedResultsState)
    assert isinstance(result.raw_results_df, pl.DataFrame)
    assert len(result.raw_results_df) > 0, "Should have at least one result row"
