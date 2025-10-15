import io
from pathlib import Path

import polars as pl
import polars.selectors as cs
import requests
from polars.testing import assert_frame_equal

from plumberlama import Config
from plumberlama.api_models import Questions
from plumberlama.documentation import (
    build_mkdocs_site,
    create_documentation_dataframe,
    create_markdown_files,
)
from plumberlama.extract.question_type import extract_question_type
from plumberlama.io.api import make_headers, preprocess_api_response
from plumberlama.io.database import query_database, save_to_database
from plumberlama.logging_config import get_logger
from plumberlama.schemas import make_results_schema
from plumberlama.states import (
    DocumentedState,
    FetchedMetadataState,
    FetchedResultsState,
    LoadedState,
    ParsedMetadataState,
    PreloadCheckState,
    ProcessedMetadataState,
    ProcessedResultsState,
)
from plumberlama.transform.cast_types import cast_results_to_schema
from plumberlama.transform.decode import decode_single_choice
from plumberlama.transform.llm import load_llm, make_generator
from plumberlama.transform.rename_results_columns import rename_results_columns
from plumberlama.transform.variable_naming import rename_vars_with_labels

logger = get_logger(__name__)


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
    logger.info("P arsing metadata...")
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
    config: Config, new_metadata: ProcessedMetadataState
) -> PreloadCheckState:
    """Check if tables exist and validate metadata consistency."""
    logger.info("Validating metadata...")
    try:
        existing_df = query_database(f"SELECT * FROM {config.survey_id}_metadata")
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
            logger.info(
                "✓ No existing tables found - creating new tables with load_counter=0"
            )
            return PreloadCheckState(load_counter=0)
        else:
            # Some other error - re-raise
            raise
    try:
        # Only check original_id and question_type - renamed 'id' can change
        assert_frame_equal(
            existing_df.sort("original_id").select(["original_id", "question_type"]),
            new_metadata.final_metadata_df.sort("original_id").select(
                ["original_id", "question_type"]
            ),
            check_row_order=True,
            check_column_order=False,
        )
    except AssertionError as e:
        raise AssertionError(
            f"Metadata schema mismatch for survey '{config.survey_id}'.\n"
            f"The survey structure has changed since the last load.\n"
            f"Details: {e}"
        ) from e

    # Get current max load_counter
    results_df = query_database(
        f"SELECT MAX(load_counter) as max_counter FROM {config.survey_id}_results"
    )
    max_counter = results_df["max_counter"][0]
    load_counter = (max_counter + 1) if max_counter is not None else 1

    logger.info(
        f"✓ Metadata validation passed - appending with load_counter={load_counter}"
    )
    return PreloadCheckState(load_counter=load_counter)


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
    processed_metadata: ProcessedMetadataState, fetched_results: FetchedResultsState
) -> ProcessedResultsState:
    """Process poll results"""
    logger.info("Processing results...")

    # Filter out incomplete and empty responses
    results_df = fetched_results.raw_results_df.filter(
        (pl.col("vCOMPLETED").cast(pl.String) != "0")
        & ~pl.all_horizontal(cs.matches("^V\\d").fill_null("").cast(pl.String) == "")
    )

    # Drop unused metadata columns
    results_df = results_df.drop(["vANONYM", "vLANG"])

    # Rename all columns using variable metadata
    results_df = rename_results_columns(
        results_df, processed_metadata.final_metadata_df
    )

    # Decode single choice (converts codes to labels)
    results_df = decode_single_choice(
        processed_metadata.processed_results_schema, results_df
    )

    # Cast columns to expected types
    results_df = cast_results_to_schema(
        results_df, processed_metadata.processed_results_schema
    )

    logger.info(f"   ✓ Processed {len(results_df)} responses")
    return ProcessedResultsState(
        results_df=results_df,
        processed_results_schema=processed_metadata.processed_results_schema,
    )


def load_data(
    proc_state: ProcessedResultsState,
    validated_state: PreloadCheckState,
    config: Config,
    meta_state: ProcessedMetadataState = None,
) -> LoadedState:
    """Load data to database with load_counter."""
    logger.info("Loading data to database...")
    # Add load_counter to results
    results_with_counter = proc_state.results_df.with_columns(
        pl.lit(validated_state.load_counter).alias("load_counter")
    )

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
        metadata_df = query_database(f"SELECT * FROM {config.survey_id}_metadata")

    loaded = save_to_database(
        results_df=results_with_counter,
        metadata_df=metadata_df,
        table_prefix=config.survey_id,
        append=append,
    )
    logger.info(
        f"   ✓ Loaded {len(results_with_counter)} responses with load_counter={validated_state.load_counter}"
    )
    return LoadedState(loaded)


def generate_doc(config: Config) -> DocumentedState:
    """Generate documentation from survey data stored in database.

    Args:
        config: Configuration object

    Returns:
        DocumentedState: State after generating documentation
    """
    logger.info("Generating documentation from database...")
    # Retrieve metadata from database
    metadata_df = query_database(f"SELECT * FROM {config.survey_id}_metadata")

    # Step 1: Prepare documentation DataFrame (metadata already contains everything)
    doc_df = create_documentation_dataframe(metadata_df)

    # Step 2: Create markdown files
    num_questions = metadata_df["question_id"].n_unique()
    create_markdown_files(
        doc_df, num_questions, config.doc_output_dir, config.survey_id
    )

    # Step 3: Build MkDocs site
    mkdocs_config = {
        "site_name": config.mkdocs_site_name,
        "site_author": config.mkdocs_site_author,
        "repo_url": config.mkdocs_repo_url,
        "logo_url": config.mkdocs_logo_url,
    }
    site_path = build_mkdocs_site(config.doc_output_dir, mkdocs_config)

    # Return DocumentedState with validated paths
    docs_path = Path(config.doc_output_dir)
    logger.info(f"   ✓ Generated documentation at {docs_path}")
    return DocumentedState(
        docs_dir=docs_path,
        site_dir=site_path,
    )
