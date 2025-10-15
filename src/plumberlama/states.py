from dataclasses import dataclass
from pathlib import Path

import pandera.polars as pa
import polars as pl

from plumberlama.generated_api_models import Questions
from plumberlama.logging_config import get_logger
from plumberlama.validation_schemas import ParsedMetadataSchema, ProcessedMetadataSchema

logger = get_logger(__name__)


@dataclass(frozen=True)
class FetchedMetadataState:
    """State after fetching metadata from the API."""

    raw_questions: list[Questions]

    def __post_init__(self):
        assert isinstance(self.raw_questions, list)
        assert all(isinstance(q, Questions) for q in self.raw_questions)


@dataclass(frozen=True)
class ParsedMetadataState:
    """State after parsing metadata into a single variable-level DataFrame with question text."""

    parsed_metadata_df: pl.DataFrame

    def __post_init__(self):
        ParsedMetadataSchema.validate(self.parsed_metadata_df, lazy=False)


@dataclass(frozen=True)
class ProcessedMetadataState:
    """State after processing metadata with LLM-generated variable names."""

    final_metadata_df: pl.DataFrame
    processed_results_schema: pa.DataFrameSchema

    def __post_init__(self):
        ProcessedMetadataSchema.validate(self.final_metadata_df, lazy=False)

        # Validate no V<number> pattern
        v_pattern_ids = self.final_metadata_df.filter(
            pl.col("id").str.starts_with("V")
            & pl.col("id").str.slice(1, 2).str.contains(r"\d")
        )
        assert (
            len(v_pattern_ids) == 0
        ), f"Found V<number> syntax that should be renamed: {v_pattern_ids['id'].to_list()}"

        # Validate no duplicate variable IDs
        duplicate_ids = (
            self.final_metadata_df.group_by("id")
            .agg(pl.len().alias("count"))
            .filter(pl.col("count") > 1)
        )
        assert (
            len(duplicate_ids) == 0
        ), f"Found duplicate variable IDs: {duplicate_ids['id'].to_list()}"

        # Validate no duplicate target names
        target_names = self.final_metadata_df["id"].to_list()
        duplicates = [n for n in set(target_names) if target_names.count(n) > 1]
        if duplicates:
            raise ValueError(f"Duplicate target variable names: {duplicates}")


@dataclass(frozen=True)
class FetchedResultsState:
    """State after fetching results data from the API."""

    raw_results_df: pl.DataFrame


@dataclass(frozen=True)
class ProcessedResultsState:
    """State after processing and validating results data."""

    results_df: pl.DataFrame
    processed_results_schema: pa.DataFrameSchema

    def __post_init__(self):
        self.processed_results_schema.validate(self.results_df, lazy=True)


@dataclass(frozen=True)
class DocumentedState:
    """State after generating documentation.

    Validates that all necessary files for hosting documentation exist.
    """

    site_dir: Path  # Path to built HTML site

    def __post_init__(self):
        from pathlib import Path

        # Convert to Path object if needed
        site_dir = Path(object.__getattribute__(self, "site_dir"))

        # Validate site directory exists (MkDocs build output)
        assert site_dir.exists(), f"MkDocs site directory not found: {site_dir}"

        # Validate key HTML files exist
        required_html_files = ["index.html"]
        for html_file in required_html_files:
            html_path = site_dir / html_file
            assert html_path.exists(), f"Required HTML file not found: {html_path}"

        # Validate assets directory exists in site (copied by MkDocs)
        site_assets = site_dir / "assets"
        if not site_assets.exists():
            logger.warning(
                f"⚠ Warning: Assets directory not found in site: {site_assets}"
            )


@dataclass(frozen=True)
class PreloadCheckState:
    """State after checking database and validating metadata before load."""

    load_counter: int

    def __post_init__(self):
        assert isinstance(self.load_counter, int)
        assert self.load_counter >= 0


@dataclass(frozen=True)
class LoadedState:
    """State after loading data to database."""

    loaded: str
