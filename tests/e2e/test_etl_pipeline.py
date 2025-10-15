import os

import pytest
from dotenv import load_dotenv

from plumberlama import run_etl_pipeline
from plumberlama.io.database import query_database
from plumberlama.logging_config import get_logger
from plumberlama.states import LoadedState

logger = get_logger(__name__)

load_dotenv()


@pytest.mark.slow
@pytest.mark.integration
def test_run_etl_pipeline_function():
    """Test the run_etl_pipeline convenience function.

    This test validates that the single-function pipeline orchestrator works
    correctly by loading config from environment variables.
    """

    loaded_state = run_etl_pipeline()

    assert isinstance(loaded_state, LoadedState)

    survey_id = os.getenv("SURVEY_ID", "test_survey")

    metadata_df = query_database(f"SELECT * FROM {survey_id}_metadata")
    assert metadata_df is not None
    assert len(metadata_df) > 0
    logger.info(f"✓ Metadata table has {len(metadata_df)} variables")

    results_df = query_database(f"SELECT * FROM {survey_id}_results")
    assert results_df is not None
    assert "load_counter" in results_df.columns
    logger.info(f"✓ Results table has {len(results_df)} responses")

    if len(results_df) > 0:
        assert all(results_df["load_counter"] == 0)
        logger.info("✓ All results have load_counter=0 (first load)")

    logger.info("✓ run_etl_pipeline function works correctly!")
