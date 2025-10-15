from plumberlama.transitions import process_poll_results


def test_process_poll_results(sample_processed_metadata, sample_loaded_results):
    """Test processing poll results through the full pipeline."""
    loaded_results = sample_loaded_results

    result = process_poll_results(sample_processed_metadata, loaded_results)
    results_df = result.results_df

    # Basic assertions
    assert results_df is not None
    assert len(results_df) > 0
