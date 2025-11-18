import os

import pytest
from dotenv import load_dotenv

from plumberlama import run_etl_pipeline
from plumberlama.config import Config
from plumberlama.io.database import query_database
from plumberlama.logging_config import get_logger
from plumberlama.states import LoadedState

logger = get_logger(__name__)

load_dotenv()


@pytest.mark.slow
@pytest.mark.integration
def test_run_etl_pipeline_function(docker_compose_test_db, db_connection, monkeypatch):
    """Test the run_etl_pipeline convenience function.

    This test validates that the single-function pipeline orchestrator works
    correctly by loading config from environment variables.

    Uses the docker_compose_test_db fixture to ensure a test database is available.
    """
    # Set environment variables to point to test database
    monkeypatch.setenv("DB_HOST", "localhost")
    monkeypatch.setenv("DB_PORT", "5433")
    monkeypatch.setenv("DB_USER", "test_user")
    monkeypatch.setenv("DB_PASSWORD", "test_password")
    monkeypatch.setenv("DB_NAME", "test_db")
    monkeypatch.setenv("SURVEY_ID", "e2e_test_survey")

    # Skip if API credentials not available
    if not os.getenv("LP_API_TOKEN") or not os.getenv("LP_POLL_ID"):
        pytest.skip(
            "Real API credentials not available (LP_API_TOKEN or LP_POLL_ID not set)"
        )

    loaded_state = run_etl_pipeline()

    assert isinstance(loaded_state, LoadedState)

    # Create config from environment variables for querying the database
    config = Config(
        survey_id=os.getenv("SURVEY_ID", "e2e_test_survey"),
        lp_poll_id=int(os.getenv("LP_POLL_ID")),
        lp_api_token=os.getenv("LP_API_TOKEN"),
        lp_api_base_url=os.getenv(
            "LP_API_BASE_URL", "https://app.lamapoll.de/assets/api/v2"
        ),
        llm_model=os.getenv("LLM_MODEL", "mistralai/mistral-small-3.2-24b-instruct"),
        llm_key=os.getenv("OR_KEY"),
        llm_base_url=os.getenv("LLM_BASE_URL", "https://openrouter.ai/api/v1"),
        processed_data_output_path=None,
        db_host="localhost",
        db_port=5433,
        db_name="test_db",
        db_user="test_user",
        db_password="test_password",
    )

    survey_id = config.survey_id

    # Verify metadata table
    metadata_df = query_database(f"SELECT * FROM {survey_id}_metadata", config=config)
    assert metadata_df is not None
    assert len(metadata_df) > 0
    logger.info(f"✓ Metadata table has {len(metadata_df)} variables")

    # Verify anonymized distributions table (for shuffled numeric values)
    distributions_df = query_database(
        f"SELECT * FROM {survey_id}_distributions", config=config
    )
    assert distributions_df is not None
    assert "load_counter" in distributions_df.columns
    assert "variable_id" in distributions_df.columns
    assert "value" in distributions_df.columns
    logger.info(f"✓ Distributions table has {len(distributions_df)} shuffled values")

    # Verify anonymized categorical table (for aggregated counts)
    categorical_df = query_database(
        f"SELECT * FROM {survey_id}_categorical", config=config
    )
    assert categorical_df is not None
    assert "load_counter" in categorical_df.columns
    assert "variable_id" in categorical_df.columns
    assert "value" in categorical_df.columns
    assert "count" in categorical_df.columns
    logger.info(f"✓ Categorical table has {len(categorical_df)} aggregated values")

    # Verify load_counter for first load
    if len(distributions_df) > 0:
        assert all(distributions_df["load_counter"] == 0)
        logger.info("✓ All distributions have load_counter=0 (first load)")

    if len(categorical_df) > 0:
        assert all(categorical_df["load_counter"] == 0)
        logger.info("✓ All categorical data have load_counter=0 (first load)")

    logger.info("✓ run_etl_pipeline function works correctly with anonymized data!")
