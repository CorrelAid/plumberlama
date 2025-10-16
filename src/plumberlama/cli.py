import inspect
import os
import sys

from click import argument, command, echo, group, option

from plumberlama.config import Config
from plumberlama.io import database_queries
from plumberlama.io.database import query_database
from plumberlama.logging_config import get_logger, setup_logging
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


def load_config_from_env() -> Config:
    """Load configuration from environment variables."""
    return Config(
        survey_id=os.getenv("SURVEY_ID", "test_survey"),
        lp_poll_id=int(os.getenv("LP_POLL_ID", "0")),
        lp_api_token=os.getenv("LP_API_TOKEN", ""),
        lp_api_base_url=os.getenv("LP_API_BASE_URL", ""),
        llm_model=os.getenv("LLM_MODEL", ""),
        llm_key=os.getenv("OR_KEY", ""),
        llm_base_url=os.getenv("LLM_BASE_URL", ""),
        site_output_dir=os.getenv("SITE_OUTPUT_DIR", ""),
        mkdocs_site_name=os.getenv("MKDOCS_SITE_NAME", ""),
        mkdocs_site_author=os.getenv("MKDOCS_SITE_AUTHOR", ""),
        mkdocs_repo_url=os.getenv("MKDOCS_REPO_URL", ""),
        mkdocs_logo_url=os.getenv("MKDOCS_LOGO_URL", ""),
        db_host=os.getenv("DB_HOST", ""),
        db_port=int(os.getenv("DB_PORT", "5432")),
        db_name=os.getenv("DB_NAME", ""),
        db_user=os.getenv("DB_USER", ""),
        db_password=os.getenv("DB_PASSWORD", ""),
    )


def run_etl_pipeline() -> LoadedState:
    """Run complete ETL pipeline: Fetch → Parse → Process → Validate → Load."""
    config = load_config_from_env()

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
    config = load_config_from_env()

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
    setup_logging(os.getenv("LOG_LEVEL", "INFO"))
    try:
        run_etl_pipeline()
        sys.exit(0)
    except Exception as e:
        echo(f"ETL pipeline failed: {e}", err=True)
        sys.exit(1)


@command()
def docs():
    """Generate documentation for the survey."""
    setup_logging(os.getenv("LOG_LEVEL", "INFO"))
    try:
        generate_docs()
        sys.exit(0)
    except Exception as e:
        echo(f"Documentation generation failed: {e}", err=True)
        sys.exit(1)


@command()
@option("--list", "list_functions", is_flag=True, help="List available query functions")
@argument("function", required=False)
@argument("args", nargs=-1)
def query(list_functions, function, args):
    """Query the database using predefined functions.

    Examples:
        plumberlama query --list
        plumberlama query get_question_metadata 1
        plumberlama query get_frequency_distribution age
    """
    setup_logging(os.getenv("LOG_LEVEL", "WARNING"))

    if list_functions:
        echo("Available query functions:\n")
        for name in dir(database_queries):
            if callable(getattr(database_queries, name)) and not name.startswith("_"):
                func = getattr(database_queries, name)
                echo(f"  {name}: {(func.__doc__ or '').strip()}")
        sys.exit(0)

    if not function:
        echo("Error: Must specify a function name or use --list", err=True)
        sys.exit(1)

    if not hasattr(database_queries, function):
        echo(f"Error: Function '{function}' not found", err=True)
        sys.exit(1)

    config = load_config_from_env()

    try:
        query_func = getattr(database_queries, function)
        sig = inspect.signature(query_func)
        params = list(sig.parameters.keys())

        if params and params[0] == "table_prefix":
            sql = query_func(config.survey_id, *args)
        else:
            sql = query_func(*args)

        result = query_database(sql, config)
        echo(result)
    except Exception as e:
        echo(f"Query failed: {e}", err=True)
        sys.exit(1)


@group()
def main():
    """plumberlama: Pipeline to process and document LamaPoll surveys."""
    pass


main.add_command(etl)
main.add_command(docs)
main.add_command(query)
