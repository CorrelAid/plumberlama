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
        site_output_dir=os.getenv("SITE_OUTPUT_DIR", "/tmp/docs"),
        mkdocs_site_name=os.getenv("MKDOCS_SITE_NAME", "Test Survey"),
        mkdocs_site_author=os.getenv("MKDOCS_SITE_AUTHOR", "Test Author"),
        mkdocs_repo_url=os.getenv("MKDOCS_REPO_URL", ""),
        mkdocs_logo_url=os.getenv("MKDOCS_LOGO_URL", ""),
        db_host="localhost",
        db_port=5433,
        db_name="test_db",
        db_user="test_user",
        db_password="test_password",
    )

    survey_id = config.survey_id

    metadata_df = query_database(f"SELECT * FROM {survey_id}_metadata", config=config)
    assert metadata_df is not None
    assert len(metadata_df) > 0
    logger.info(f"✓ Metadata table has {len(metadata_df)} variables")

    results_df = query_database(f"SELECT * FROM {survey_id}_results", config=config)
    assert results_df is not None
    assert "load_counter" in results_df.columns
    logger.info(f"✓ Results table has {len(results_df)} responses")

    if len(results_df) > 0:
        assert all(results_df["load_counter"] == 0)
        logger.info("✓ All results have load_counter=0 (first load)")

    logger.info("✓ run_etl_pipeline function works correctly!")
