import os

from plumberlama import Config
from plumberlama.logging_config import get_logger
from plumberlama.states import LoadedState
from plumberlama.transitions import (
    fetch_poll_metadata,
    fetch_poll_results,
    load_data,
    parse_poll_metadata,
    preload_check,
    process_poll_metadata,
    process_poll_results,
)

logger = get_logger(__name__)


def run_etl_pipeline() -> LoadedState:
    """Run complete ETL pipeline: Fetch → Parse → Process → Validate → Load.

    Loads configuration from environment variables.
    """
    config = Config(
        survey_id=os.getenv("SURVEY_ID", "test_survey"),
        lp_poll_id=int(os.getenv("LP_POLL_ID")),
        lp_api_token=os.getenv("LP_API_TOKEN"),
        lp_api_base_url=os.getenv("LP_API_BASE_URL"),
        llm_model=os.getenv("LLM_MODEL"),
        llm_key=os.getenv("OR_KEY"),
        llm_base_url=os.getenv("LLM_BASE_URL"),
        doc_output_dir=os.getenv("DOC_OUTPUT_DIR", "/tmp/docs"),
        mkdocs_site_name=os.getenv("MKDOCS_SITE_NAME"),
        mkdocs_site_author=os.getenv("MKDOCS_SITE_AUTHOR"),
        mkdocs_repo_url=os.getenv("MKDOCS_REPO_URL"),
        mkdocs_logo_url=os.getenv("MKDOCS_LOGO_URL"),
    )

    logger.info("=" * 60)
    logger.info(f"Starting ETL Pipeline for survey: {config.survey_id}")
    logger.info("=" * 60)

    current_metadata = process_poll_metadata(
        parse_poll_metadata(fetch_poll_metadata(config)), config
    )

    validated_metadata = preload_check(config, current_metadata)

    results = process_poll_results(current_metadata, fetch_poll_results(config))

    loaded_state = load_data(
        results, validated_metadata, config, meta_state=current_metadata
    )

    logger.info("=" * 60)
    logger.info("ETL Pipeline completed successfully!")
    logger.info(f"Load counter: {validated_metadata.load_counter}")
    logger.info("=" * 60)

    return loaded_state


def generate_docs() -> LoadedState:
    """Run complete ETL pipeline: Fetch → Parse → Process → Validate → Load.

    Loads configuration from environment variables.
    """
    config = Config(
        survey_id=os.getenv("SURVEY_ID", "test_survey"),
        lp_poll_id=int(os.getenv("LP_POLL_ID")),
        lp_api_token=os.getenv("LP_API_TOKEN"),
        lp_api_base_url=os.getenv("LP_API_BASE_URL"),
        doc_output_dir=os.getenv("DOC_OUTPUT_DIR", "/tmp/docs"),
        mkdocs_site_name=os.getenv("MKDOCS_SITE_NAME"),
        mkdocs_site_author=os.getenv("MKDOCS_SITE_AUTHOR"),
        mkdocs_repo_url=os.getenv("MKDOCS_REPO_URL"),
        mkdocs_logo_url=os.getenv("MKDOCS_LOGO_URL"),
    )

    logger.info("=" * 60)
    logger.info(f"Starting ETL Pipeline for survey: {config.survey_id}")
    logger.info("=" * 60)

    current_metadata = process_poll_metadata(
        parse_poll_metadata(fetch_poll_metadata(config)), config
    )

    validated_metadata = preload_check(config, current_metadata)

    results = process_poll_results(current_metadata, fetch_poll_results(config))

    loaded_state = load_data(
        results, validated_metadata, config, meta_state=current_metadata
    )

    logger.info("=" * 60)
    logger.info("ETL Pipeline completed successfully!")
    logger.info(f"Load counter: {validated_metadata.load_counter}")
    logger.info("=" * 60)

    return loaded_state
