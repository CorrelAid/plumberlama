import os
from pathlib import Path

import polars as pl
from mkdocs.commands.build import build

from plumberlama.logging_config import get_logger

logger = get_logger(__name__)


def create_documentation_dataframe(
    metadata_df: pl.DataFrame,
) -> pl.DataFrame:
    """Create documentation DataFrame from metadata (already contains question text)."""
    # Prepare type display - convert object to string representation
    # Note: Using map_elements because schema_variable_type is Object type (Polars DataType objects)
    doc_df = metadata_df.with_columns(
        [
            pl.col("schema_variable_type")
            .map_elements(
                lambda x: str(x).replace("DataType", ""), return_dtype=pl.String
            )
            .alias("type_display")
        ]
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

    # Format possible values from struct column or string
    # When loaded from database, dicts are stored as strings
    doc_df = doc_df.with_columns(
        [
            pl.when(pl.col("possible_values").is_not_null())
            .then(
                pl.col("possible_values").map_elements(
                    lambda x: (
                        "; ".join(str(v) for v in x.values() if v)
                        if isinstance(x, dict)
                        else x if isinstance(x, str) else ""
                    ),
                    return_dtype=pl.String,
                )
            )
            .otherwise(pl.lit(""))
            .alias("possible_values_display")
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


def build_mkdocs_site(output_path: str, mkdocs_config: dict = None) -> Path:
    """Build MkDocs static site from markdown files using MkDocs Python API.

    Args:
        output_path: Directory path containing markdown documentation files
        mkdocs_config: Optional dict with site_name, site_author, repo_url, logo_url

    Returns:
        Path to the built site directory

    Raises:
        ImportError: If mkdocs package is not installed
        ValueError: If mkdocs_config is not provided
        Exception: If MkDocs build fails
    """
    docs_path = Path(output_path).resolve()
    project_root = docs_path.parent
    site_dir = project_root / "site"

    if not mkdocs_config:
        raise ValueError("mkdocs_config must be provided to build documentation")

    # Write CSS file
    css_content = """/*-- Main Color --*/

:root {
  --md-primary-fg-color: #991766;
  --md-accent-fg-color: #991766;
}

/*-- Logo Size --*/

.md-header__button.md-logo img,
.md-header__button.md-logo svg {
  height: 3rem;
  width: auto;
}

/*-- Table Borders --*/

table {
  border-collapse: collapse;
  width: 100%;
}

table th,
table td {
  border: 1px solid var(--md-default-fg-color--lightest);
  padding: 0.6rem 1rem;
}

table thead th {
  border-bottom: 2px solid var(--md-default-fg-color--light);
  background-color: var(--md-code-bg-color);
}

/* Dark mode adjustments */
[data-md-color-scheme="slate"] table th,
[data-md-color-scheme="slate"] table td {
  border-color: var(--md-default-fg-color--lighter);
}

[data-md-color-scheme="slate"] table thead th {
  border-bottom-color: var(--md-default-fg-color);
}
"""

    # Create stylesheets directory and write CSS
    stylesheets_dir = docs_path / "stylesheets"
    stylesheets_dir.mkdir(exist_ok=True)
    (stylesheets_dir / "extra.css").write_text(css_content, encoding="utf-8")

    try:
        # Import MkDocs modules (note: this is not an official public API)
        from mkdocs.config.defaults import MkDocsConfig

        # Build full config dict with defaults
        full_config = {
            "site_name": mkdocs_config.get("site_name", "Survey Documentation"),
            "site_description": f"Documentation for {mkdocs_config.get('site_name', 'Survey Documentation')}",
            "site_author": mkdocs_config.get("site_author", "Survey Team"),
            "theme": {
                "name": "material",
                "logo": mkdocs_config.get("logo_url"),
                "features": [
                    "navigation.instant",
                    "navigation.tracking",
                    "navigation.tabs",
                    "navigation.sections",
                    "navigation.expand",
                    "navigation.top",
                    "search.suggest",
                    "search.highlight",
                    "search.share",
                    "toc.follow",
                    "content.code.copy",
                ],
                "palette": [
                    {
                        "media": "(prefers-color-scheme: light)",
                        "scheme": "default",
                        "primary": "custom",
                        "accent": "custom",
                        "toggle": {
                            "icon": "material/brightness-7",
                            "name": "Switch to dark mode",
                        },
                    },
                    {
                        "media": "(prefers-color-scheme: dark)",
                        "scheme": "slate",
                        "primary": "custom",
                        "accent": "custom",
                        "toggle": {
                            "icon": "material/brightness-4",
                            "name": "Switch to light mode",
                        },
                    },
                ],
            },
            "nav": [
                {"Home": "index.md"},
                {"Survey Documentation": "survey_documentation.md"},
            ],
            "markdown_extensions": [
                "tables",
                {"toc": {"permalink": True, "toc_depth": 3}},
                "admonition",
                "pymdownx.details",
                "pymdownx.superfences",
                {"pymdownx.tabbed": {"alternate_style": True}},
                {"pymdownx.highlight": {"anchor_linenums": True}},
                "pymdownx.inlinehilite",
                "pymdownx.snippets",
                "attr_list",
                "md_in_html",
            ],
            "plugins": [{"search": {"lang": "en", "separator": r"[\s\-\.]"}}],
            "docs_dir": str(docs_path),
            "site_dir": str(site_dir),
            "extra_css": ["stylesheets/extra.css"],
        }

        # Add repo_url if provided
        if mkdocs_config.get("repo_url"):
            full_config["repo_url"] = mkdocs_config["repo_url"]
            full_config["edit_uri"] = "edit/main/docs/"

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

    except ImportError:
        logger.error(
            "✗ MkDocs not found. Install with: pip install mkdocs mkdocs-material"
        )
        raise
    except Exception as e:
        logger.error(f"✗ MkDocs build failed: {e}")
        raise
