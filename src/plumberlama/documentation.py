import os
from pathlib import Path

import polars as pl
import requests
from mkdocs.commands.build import build
from mkdocs.config.defaults import (
    MkDocsConfig,
)  # (note: this is not an official public API)

from plumberlama.config import build_mkdoc_config, css_content
from plumberlama.logging_config import get_logger

logger = get_logger(__name__)


def create_documentation_dataframe(
    metadata_df: pl.DataFrame,
) -> pl.DataFrame:
    """Create documentation DataFrame from metadata"""

    doc_df = metadata_df.with_columns(
        [pl.col("schema_variable_type").alias("type_display")]
    )

    # Format range information from range_min and range_max
    doc_df = doc_df.with_columns(
        [
            pl.when(
                pl.col("range_min").is_not_null() | pl.col("range_max").is_not_null()
            )
            .then(
                pl.concat_str(
                    [
                        pl.lit("["),
                        pl.col("range_min").cast(pl.String).fill_null("None"),
                        pl.lit(", "),
                        pl.col("range_max").cast(pl.String).fill_null("None"),
                        pl.lit("]"),
                    ]
                )
            )
            .otherwise(pl.lit(""))
            .alias("range_display")
        ]
    )

    # Format possible values from list column
    doc_df = doc_df.with_columns(
        [
            pl.when(pl.col("possible_values_labels").is_not_null())
            .then(pl.col("possible_values_labels").list.join("; "))
            .otherwise(pl.lit(""))
            .alias("possible_values_display")
        ]
    )

    # Format scale labels from list column
    doc_df = doc_df.with_columns(
        [
            pl.when(pl.col("scale_labels").is_not_null())
            .then(pl.col("scale_labels").list.join("; "))
            .otherwise(pl.lit(""))
            .alias("scale_labels_display")
        ]
    )

    # Select relevant columns and rename 'id' to 'variable'
    doc_df = doc_df.select(
        [
            pl.col("id").alias("variable"),
            pl.col("label").fill_null(""),
            pl.col("question_text").alias("question"),
            pl.col("question_id"),
            pl.col("question_type"),
            pl.col("type_display").alias("data_type"),
            pl.col("range_display").alias("range"),
            pl.col("possible_values_display").alias("possible_values"),
            pl.col("scale_labels_display").alias("scale_labels"),
            pl.col("question_position"),
        ]
    )

    # Rename columns: replace underscores with spaces and title case
    doc_df = doc_df.rename(lambda col: col.replace("_", " ").title())

    return doc_df.sort("Question Position")


def create_markdown_files(
    doc_df: pl.DataFrame, number_questions: int, output_path: str, survey_id: str
) -> None:
    """Create markdown documentation files from documentation DataFrame."""

    os.makedirs(output_path, exist_ok=True)

    markdown = "# Survey Documentation\n\n"
    markdown += "## Overview\n\n"
    markdown += f"Total questions: {number_questions}\n\n"
    markdown += f"Total variables: {doc_df.height}\n\n"
    markdown += "## Questions\n\n"

    with pl.Config(
        tbl_formatting="MARKDOWN",
        tbl_hide_column_data_types=True,
        tbl_hide_dataframe_shape=True,
        tbl_rows=-1,
        tbl_width_chars=1000,
        fmt_str_lengths=1000,
    ):
        for question_id in doc_df["Question Id"].unique().sort():
            question_vars = doc_df.filter(pl.col("Question Id") == question_id)
            first_var = question_vars.row(0, named=True)
            question_text = first_var["Question"]
            question_type = first_var["Question Type"]
            question_position = first_var["Question Position"]

            markdown += f"### Q{question_position}: {question_text}\n\n"
            markdown += f"**Type:** {question_type}\n\n"
            markdown += f"**Variables:** {question_vars.height}\n\n"

            if question_type == "scale":
                cols = ["Variable", "Data Type", "Range"]
            elif question_type == "matrix":
                cols = ["Variable", "Label", "Data Type", "Range"]
            elif question_type == "single_choice":
                cols = ["Variable", "Data Type", "Possible Values"]
            elif question_type in ["input_single_singleline", "input_single_multiline"]:
                cols = ["Variable", "Data Type"]
            elif question_type.startswith("input_") or question_type.startswith(
                "multiple_choice"
            ):
                cols = ["Variable", "Label", "Data Type"]
            else:
                cols = ["Variable", "Label", "Data Type"]

            available_cols = [c for c in cols if c in question_vars.columns]
            table_str = str(question_vars.select(available_cols))
            markdown += table_str + "\n\n"

    with open(
        os.path.join(output_path, "survey_documentation.md"), "w", encoding="utf-8"
    ) as f:
        f.write(markdown)

    index_content = f"""# Survey Documentation

Welcome to the {survey_id} survey documentation. This documentation provides detailed information about the survey questions, variables, and data structure.

## Quick Navigation

- **[Survey Documentation](survey_documentation.md)** - Complete list of all questions and variables

## Overview

This documentation is automatically generated from the survey definition and includes:

- Question text and types
- Variable names and data types
- Valid ranges for numeric questions
- Possible values for choice questions
- Variable labels for multi-variable questions

## About

- **Total Questions:** {number_questions}
- **Total Variables:** {doc_df.height}

Use the search function (top right) to quickly find specific questions or variables.
"""

    with open(os.path.join(output_path, "index.md"), "w", encoding="utf-8") as f:
        f.write(index_content)


def build_mkdocs_site(
    output_path: str, mkdocs_config: dict, site_output_dir: str
) -> Path:
    """Build MkDocs static site from markdown files using MkDocs Python API."""

    docs_path = Path(output_path).resolve()
    site_dir = Path(site_output_dir).resolve()

    # Create stylesheets directory and write CSS
    stylesheets_dir = docs_path / "stylesheets"
    stylesheets_dir.mkdir(exist_ok=True)
    (stylesheets_dir / "extra.css").write_text(css_content, encoding="utf-8")

    # Download logo if URL provided
    logo_path = None
    if mkdocs_config["logo_url"]:
        try:
            response = requests.get(mkdocs_config["logo_url"], timeout=10)
            response.raise_for_status()
            logo_path = docs_path / "logo.svg"
            logo_path.write_bytes(response.content)
            logger.info(f"✓ Logo downloaded to {logo_path}")
        except Exception as e:
            logger.warning(
                f"Failed to download logo from {mkdocs_config['logo_url']}: {e}"
            )
            logo_path = None

    # Build full config dict with defaults
    full_config = build_mkdoc_config(
        docs_path,
        site_dir,
        mkdocs_config["site_name"],
        mkdocs_config["site_author"],
        "logo.svg" if logo_path else None,
    )

    # Create MkDocsConfig object and load from dict
    config = MkDocsConfig(config_file_path=None)
    config.load_dict(full_config)

    # Validate the config (this initializes plugins and other components)
    errors, warnings = config.validate()
    if errors:
        raise ValueError(f"MkDocs config validation errors: {errors}")

    # Run plugin startup event
    config["plugins"].run_event("startup", command="build", dirty=False)

    try:
        # Build the site
        build(config)
    finally:
        # Always run plugin shutdown event
        config["plugins"].run_event("shutdown")

    logger.info(f"✓ MkDocs site built successfully at {site_dir}")
    return site_dir
