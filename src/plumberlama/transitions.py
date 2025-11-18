import io

import polars as pl
import polars.selectors as cs
import requests
from polars.testing import assert_frame_equal

from plumberlama.config import Config
from plumberlama.extract.question_type import extract_question_type
from plumberlama.generated_api_models import Questions
from plumberlama.io.api import make_headers, preprocess_api_response
from plumberlama.io.database import query_database, save_to_database
from plumberlama.logging_config import get_logger
from plumberlama.states import (
    AnonymizedResultsState,
    FetchedMetadataState,
    FetchedResultsState,
    LoadedState,
    ParsedMetadataState,
    PreloadCheckState,
    ProcessedMetadataState,
    ProcessedResultsState,
)
from plumberlama.transform.anonymization import anonymize_results as transform_anonymize
from plumberlama.transform.cast_types import cast_results_to_schema
from plumberlama.transform.decode import decode_single_choice
from plumberlama.transform.llm import load_llm, make_generator
from plumberlama.transform.rename_results_columns import rename_results_columns
from plumberlama.transform.variable_naming import rename_vars_with_labels
from plumberlama.validation_schemas import (
    anonymization_types,
    make_results_schema,
    question_types,
)

logger = get_logger(__name__)


class MetadataMismatchError(Exception):
    """Raised when survey metadata doesn't match existing database schema."""

    pass


def fetch_poll_metadata(config: Config) -> FetchedMetadataState:
    """Fetch poll metadata from API and validate with Pydantic models."""
    logger.info("Fetching metadata from API...")
    headers = make_headers(config.lp_api_token)
    response = requests.get(
        f"{config.lp_api_base_url}/polls/{config.lp_poll_id}/questions", headers=headers
    )
    response.raise_for_status()
    metadata_raw = response.json()

    # Preprocess API response to fix inconsistencies with api models
    metadata_preprocessed = preprocess_api_response(metadata_raw)

    # Validate API response with Pydantic models
    raw_questions = [Questions(**q) for q in metadata_preprocessed]

    logger.info(f"   ✓ Fetched {len(raw_questions)} questions")
    return FetchedMetadataState(raw_questions=raw_questions)


def parse_poll_metadata(
    loaded_metadata: FetchedMetadataState,
) -> ParsedMetadataState:
    """Parse poll metadata from validated Questions into a single metadata DataFrame at variable level."""
    logger.info("Parsing metadata...")
    questions = loaded_metadata.raw_questions

    # Create page number mapping
    unique_pages = sorted(set(q.pageId for q in questions))
    page_mapping = {page_id: idx + 1 for idx, page_id in enumerate(unique_pages)}

    question_data = []
    all_variables = []
    for abs_position, question in enumerate(questions, start=1):
        # extract question data and variables
        question_dict, vars_for_question = extract_question_type(
            question, abs_position, page_mapping[question.pageId]
        )
        question_data.append(question_dict)
        all_variables.extend(vars_for_question)

    # Create DataFrames
    question_df = pl.DataFrame(question_data)
    variable_df = pl.DataFrame(all_variables)

    # Join question text into variable-level DataFrame
    parsed_metadata_df = variable_df.join(
        question_df.select(["id", "text"]),
        left_on="question_id",
        right_on="id",
        how="left",
    ).rename({"text": "question_text"})

    # Cast enum columns from String to Enum (Polars infers them as String from dicts)
    parsed_metadata_df = parsed_metadata_df.with_columns(
        [
            pl.col("question_type").cast(pl.Enum(question_types)),
            pl.col("anonymization_type").cast(pl.Enum(anonymization_types)),
        ]
    )

    logger.info(
        f"   ✓ Parsed {len(parsed_metadata_df)} variables from {len(questions)} questions"
    )
    return ParsedMetadataState(
        parsed_metadata_df=parsed_metadata_df,
    )


def process_poll_metadata(
    parsed_metadata: ParsedMetadataState,
    config: Config,
) -> ProcessedMetadataState:
    """Process metadata by renaming variables with LLM-generated names."""
    logger.info("Processing metadata (LLM variable naming)...")
    # Generate variable names with LLM
    llm = load_llm(config.llm_model, config.llm_key, config.llm_base_url)

    generator = make_generator()
    final_metadata_df = rename_vars_with_labels(
        parsed_metadata.parsed_metadata_df,
        generator,
        llm,
    )

    processed_results_schema = make_results_schema(final_metadata_df)

    logger.info(
        f"   ✓ Processed {len(final_metadata_df)} variables with LLM-generated names"
    )
    return ProcessedMetadataState(
        final_metadata_df=final_metadata_df,
        processed_results_schema=processed_results_schema,
    )


def preload_check(
    config: Config, new_metadata: ParsedMetadataState
) -> PreloadCheckState:
    """Check if tables exist and validate metadata consistency.

    Args:
        config: Configuration object
        new_metadata: Parsed metadata state (with original variable IDs, before LLM processing)

    Returns:
        PreloadCheckState with load_counter and existing_metadata_df if appending
    """

    logger.info("Validating metadata...")
    try:
        existing_df = query_database(
            f"SELECT * FROM {config.survey_id}_metadata", config
        )
        # Cast enum columns from String to Enum after loading from DB
        existing_df = existing_df.with_columns(
            [
                pl.col("question_type").cast(pl.Enum(question_types)),
                pl.col("anonymization_type").cast(pl.Enum(anonymization_types)),
            ]
        )
        # Add anonymized_table column if it doesn't exist (for backward compatibility)
        if "anonymized_table" not in existing_df.columns:
            existing_df = existing_df.with_columns(
                pl.when(pl.col("anonymization_type") == "shuffle")
                .then(pl.lit("_distributions"))
                .when(pl.col("anonymization_type") == "aggregate")
                .then(pl.lit("_categorical"))
                .otherwise(pl.lit(None))
                .alias("anonymized_table")
            )
    except Exception as e:
        # Check if it's a "table doesn't exist" error
        error_msg = str(e).lower()
        if (
            "table" in error_msg
            or "relation" in error_msg
            or "not found" in error_msg
            or "does not exist" in error_msg
        ):
            # Tables don't exist - first load
            logger.info("✓ No existing tables found - first load detected")
            logger.info("  → load_counter=0: Will CREATE new tables with LLM naming")
            return PreloadCheckState(load_counter=0)
        else:
            # Some other error - re-raise
            raise
    try:
        # Compare parsed metadata (id field) with existing database metadata (original_id field)
        # The database has processed metadata with original_id, we have parsed metadata with id
        # Rename existing original_id to id for comparison
        existing_comparison = existing_df.sort("original_id").select(
            [pl.col("original_id").alias("id"), "question_type"]
        )
        new_comparison = new_metadata.parsed_metadata_df.sort("id").select(
            ["id", "question_type"]
        )

        assert_frame_equal(
            existing_comparison,
            new_comparison,
            check_row_order=True,
            check_column_order=False,
        )
    except AssertionError as e:
        logger.error("=" * 60)
        logger.error("❌ PRELOAD CHECK FAILED - PIPELINE STOPPED")
        logger.error("=" * 60)
        logger.error(f"Survey structure has changed for '{config.survey_id}'")
        logger.error(
            "The survey cannot be loaded because questions/variables differ from existing data."
        )
        logger.error("This prevents data corruption and maintains schema consistency.")
        logger.error("")
        logger.error("To proceed, either:")
        logger.error("  1. Restore the original survey structure in LamaPoll")
        logger.error("  2. Drop existing tables to start fresh (data loss!)")
        logger.error("=" * 60)
        raise MetadataMismatchError(
            f"Metadata schema mismatch for survey '{config.survey_id}'.\n"
            f"The survey structure has changed since the last load.\n"
            f"Details: {e}"
        ) from e

    # Get current max load_counter from distributions table
    distributions_df = query_database(
        f"SELECT MAX(load_counter) as max_counter FROM {config.survey_id}_distributions",
        config,
    )
    max_counter = distributions_df["max_counter"][0]
    load_counter = (max_counter + 1) if max_counter is not None else 1

    logger.info("✓ Metadata validation passed - existing tables found")
    logger.info(
        f"  → load_counter={load_counter}: Will APPEND new anonymized data to existing data"
    )
    logger.info("  → Using existing variable names from database for consistency")
    return PreloadCheckState(
        load_counter=load_counter, existing_metadata_df=existing_df
    )


def fetch_poll_results(config: Config) -> FetchedResultsState:
    logger.info("Fetching results from API...")
    headers = make_headers(config.lp_api_token)
    response = requests.get(
        f"{config.lp_api_base_url}/polls/{config.lp_poll_id}/legacyResults",
        headers=headers,
    )
    response.raise_for_status()
    results_raw = response.json()["data"]
    raw_results_df = pl.read_csv(io.StringIO(results_raw))

    logger.info(f"   ✓ Fetched {len(raw_results_df)} responses")
    return FetchedResultsState(raw_results_df=raw_results_df)


def process_poll_results(
    processed_metadata: ProcessedMetadataState,
    fetched_results: FetchedResultsState,
    existing_metadata_df: pl.DataFrame = None,
) -> ProcessedResultsState:
    """Process poll results.

    Args:
        processed_metadata: Newly processed metadata with LLM-generated names (None for append mode)
        fetched_results: Raw results from API
        existing_metadata_df: Existing metadata from DB (for append mode).
                             If provided, uses these variable names instead of new ones.
    """
    logger.info("Processing results...")

    # Filter out incomplete and empty responses
    results_df = fetched_results.raw_results_df.filter(
        (pl.col("vCOMPLETED").cast(pl.String) != "0")
        & ~pl.all_horizontal(cs.matches("^V\\d").fill_null("").cast(pl.String) == "")
    )

    # Drop unused metadata columns
    results_df = results_df.drop(["vANONYM", "vLANG"])

    # Determine which metadata to use
    if existing_metadata_df is not None:
        # Append mode: Use existing database metadata
        metadata_for_naming = existing_metadata_df
        # Create schema from existing metadata
        processed_results_schema = make_results_schema(existing_metadata_df)
    else:
        # First load: Use newly processed metadata
        assert (
            processed_metadata is not None
        ), "processed_metadata required when not appending"
        metadata_for_naming = processed_metadata.final_metadata_df
        processed_results_schema = processed_metadata.processed_results_schema

    # Rename all columns using variable metadata
    results_df = rename_results_columns(results_df, metadata_for_naming)

    # Decode single choice (converts codes to labels)
    results_df = decode_single_choice(
        processed_results_schema,
        results_df,
        metadata_for_naming,
    )

    # Cast columns to expected types
    results_df = cast_results_to_schema(results_df, processed_results_schema)

    logger.info(f"   ✓ Processed {len(results_df)} responses")
    return ProcessedResultsState(
        results_df=results_df,
        processed_results_schema=processed_results_schema,
    )


def save_processed_parquet(
    proc_state: ProcessedResultsState, config: Config, load_counter: int
) -> None:
    """Save processed results to parquet file before anonymization.

    Args:
        proc_state: Processed results state
        config: Configuration with processed_data_output_path
    """
    from pathlib import Path

    if not config.processed_data_output_path:
        return

    output_path = Path(config.processed_data_output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    # Add load_counter to filename: data.parquet -> data_0.parquet
    output_path = output_path.with_stem(f"{output_path.stem}_{load_counter}")

    logger.info(f"Saving processed data to parquet: {output_path}")
    proc_state.results_df.write_parquet(output_path)
    logger.info(f"   ✓ Saved {len(proc_state.results_df)} rows to {output_path}")


def anonymize_results(
    proc_state: ProcessedResultsState,
    metadata_df: pl.DataFrame,
    load_counter: int,
) -> AnonymizedResultsState:
    """Anonymize processed results by shuffling or aggregating based on question type.

    Args:
        proc_state: Processed results state (not modified)
        metadata_df: Metadata DataFrame with variable information
        load_counter: Load counter to track data waves

    Returns:
        AnonymizedResultsState with distributions and categorical DataFrames
    """
    # Call the anonymization transform (pure function, no side effects)
    distributions_df, categorical_df = transform_anonymize(
        proc_state.results_df, metadata_df, load_counter
    )

    return AnonymizedResultsState(
        distributions_df=distributions_df, categorical_df=categorical_df
    )


def load_data(
    anonymized_state: AnonymizedResultsState,
    validated_state: PreloadCheckState,
    config: Config,
    meta_state: ProcessedMetadataState = None,
) -> LoadedState:
    """Load anonymized data to database.

    Args:
        anonymized_state: Anonymized results state with distributions and categorical DataFrames
        validated_state: Preload check state with load_counter
        config: Configuration
        meta_state: Processed metadata state (required for first load)

    Returns:
        LoadedState indicating success
    """
    logger.info("Loading anonymized data to database...")

    # Determine if we should append (load_counter > 0) or create new (load_counter == 0)
    append = validated_state.load_counter > 0

    # For first load (load_counter == 0), we need metadata to create tables
    # For subsequent loads (load_counter > 0), metadata already exists in DB
    if validated_state.load_counter == 0:
        if meta_state is None:
            raise ValueError(
                "meta_state is required when load_counter is 0 (first load)"
            )
        metadata_df = meta_state.final_metadata_df
    else:
        # Retrieve metadata from database - don't save it again
        metadata_df = query_database(
            f"SELECT * FROM {config.survey_id}_metadata", config
        )
        # Cast enum columns from String to Enum after loading from DB
        metadata_df = metadata_df.with_columns(
            [
                pl.col("question_type").cast(pl.Enum(question_types)),
                pl.col("anonymization_type").cast(pl.Enum(anonymization_types)),
            ]
        )
        # Add anonymized_table column if it doesn't exist (for backward compatibility)
        if "anonymized_table" not in metadata_df.columns:
            metadata_df = metadata_df.with_columns(
                pl.when(pl.col("anonymization_type") == "shuffle")
                .then(pl.lit("_distributions"))
                .when(pl.col("anonymization_type") == "aggregate")
                .then(pl.lit("_categorical"))
                .otherwise(pl.lit(None))
                .alias("anonymized_table")
            )

    loaded = save_to_database(
        distributions_df=anonymized_state.distributions_df,
        categorical_df=anonymized_state.categorical_df,
        metadata_df=metadata_df,
        table_prefix=config.survey_id,
        append=append,
        config=config,
    )
    logger.info(
        f"   ✓ Loaded {len(anonymized_state.distributions_df)} distribution records and "
        f"{len(anonymized_state.categorical_df)} categorical records with load_counter={validated_state.load_counter}"
    )
    return LoadedState(loaded)
