import os
import sys

from click import command, echo, group

from plumberlama.config import Config
from plumberlama.logging_config import get_logger
from plumberlama.states import LoadedState
from plumberlama.transitions import (
    MetadataMismatchError,
    TableNotFoundError,
    fetch_poll_metadata,
    fetch_poll_results,
    generate_doc,
    load_data,
    parse_poll_metadata,
    preload_check,
    process_poll_metadata,
    process_poll_results,
)

logger = get_logger(__name__)


def run_etl_pipeline() -> LoadedState:
    """Run complete ETL pipeline: Fetch → Parse → Process → Validate → Load."""
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
        db_host=os.getenv("DB_HOST"),
        db_port=int(os.getenv("DB_PORT")) if os.getenv("DB_PORT") else None,
        db_name=os.getenv("DB_NAME"),
        db_user=os.getenv("DB_USER"),
        db_password=os.getenv("DB_PASSWORD"),
    )

    logger.info("=" * 60)
    logger.info(f"Starting ETL Pipeline for survey: {config.survey_id}")
    logger.info("=" * 60)

    current_metadata = process_poll_metadata(
        parse_poll_metadata(fetch_poll_metadata(config)), config
    )

    try:
        validated_metadata = preload_check(config, current_metadata)
    except MetadataMismatchError:
        logger.error("Pipeline aborted due to preload check failure")
        raise

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
    """Generate documentation from survey data."""

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
        db_host=os.getenv("DB_HOST"),
        db_port=int(os.getenv("DB_PORT")) if os.getenv("DB_PORT") else None,
        db_name=os.getenv("DB_NAME"),
        db_user=os.getenv("DB_USER"),
        db_password=os.getenv("DB_PASSWORD"),
    )

    try:
        generate_doc(config)
    except TableNotFoundError:
        logger.error("Documentation generation aborted due to missing tables")
        raise

    logger.info("=" * 60)
    logger.info("Documentation generated successfully!")
    logger.info("=" * 60)


@command()
def etl():
    """Run the ETL pipeline to fetch, process, and load survey data."""
    try:
        run_etl_pipeline()
        sys.exit(0)
    except Exception as e:
        echo(f"ETL pipeline failed: {e}", err=True)
        sys.exit(1)


@command()
def docs():
    """Generate documentation for the survey."""
    try:
        generate_docs()
        sys.exit(0)
    except Exception as e:
        echo(f"Documentation generation failed: {e}", err=True)
        sys.exit(1)


@group()
def main():
    """plumberlama: Pipeline to process and document LamaPoll surveys."""
    pass


main.add_command(etl)
main.add_command(docs)
