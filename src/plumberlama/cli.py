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
    anonymize_results,
    fetch_poll_metadata,
    fetch_poll_results,
    load_data,
    parse_poll_metadata,
    preload_check,
    process_poll_metadata,
    process_poll_results,
    save_processed_parquet,
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
        db_host=os.getenv("DB_HOST", ""),
        db_port=int(os.getenv("DB_PORT", "5432")),
        db_name=os.getenv("DB_NAME", ""),
        db_user=os.getenv("DB_USER", ""),
        db_password=os.getenv("DB_PASSWORD", ""),
        processed_data_output_path=os.getenv("PROCESSED_DATA_OUTPUT_PATH"),
    )


def run_etl_pipeline(config: Config | None = None) -> LoadedState:
    """Run complete ETL pipeline: Fetch → Parse → Validate → Process → (Optional: Save) → Anonymize → Load.

    Args:
        config: Pipeline configuration. Loaded from environment variables if omitted.
    """
    if config is None:
        config = load_config_from_env()

    logger.info("=" * 60)
    logger.info(f"Starting ETL Pipeline for survey: {config.survey_id}")
    logger.info("=" * 60)

    # Fetch and parse metadata
    parsed_metadata = parse_poll_metadata(fetch_poll_metadata(config))

    # Check if we need to create tables or append
    try:
        validated_metadata = preload_check(config, parsed_metadata)
    except MetadataMismatchError:
        logger.error("Pipeline aborted due to preload check failure")
        raise

    # Only run LLM processing on first load (load_counter == 0)
    # For subsequent loads, use existing processed metadata from database
    if validated_metadata.load_counter == 0:
        # First load: Process metadata with LLM variable naming
        processed_metadata = process_poll_metadata(parsed_metadata, config)
        metadata_for_results = processed_metadata
    else:
        # Subsequent loads: Use existing metadata from database
        processed_metadata = None
        metadata_for_results = (
            None  # Will use existing_metadata_df from validated_metadata
        )

    # Process results using appropriate metadata
    results = process_poll_results(
        metadata_for_results,
        fetch_poll_results(config),
        existing_metadata_df=validated_metadata.existing_metadata_df,
    )

    # OPTIONAL: Save processed data to parquet before anonymization
    if config.processed_data_output_path:
        save_processed_parquet(results, config, validated_metadata.load_counter)

    # Determine which metadata to use for anonymization
    if validated_metadata.load_counter == 0:
        metadata_for_anonymization = processed_metadata.final_metadata_df
    else:
        metadata_for_anonymization = validated_metadata.existing_metadata_df

    # Anonymize results (shuffle/aggregate based on question type)
    anonymized = anonymize_results(
        results, metadata_for_anonymization, validated_metadata.load_counter
    )

    # Load anonymized data (metadata only inserted on first load)
    loaded_state = load_data(
        anonymized, validated_metadata, config, meta_state=processed_metadata
    )

    logger.info("=" * 60)
    logger.info("ETL Pipeline completed successfully!")
    logger.info(f"Load counter: {validated_metadata.load_counter}")
    logger.info("=" * 60)

    return loaded_state


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
    """plumberlama: Pipeline to process LamaPoll surveys."""
    pass


main.add_command(etl)
main.add_command(query)
