"""Integration tests for documentation generation from database.

Tests the generate_doc function which retrieves metadata from the database
and generates complete documentation site.
"""

import tempfile
from pathlib import Path

import polars as pl
import pytest

from plumberlama.config import Config
from plumberlama.io.database import save_to_database
from plumberlama.transitions import generate_doc


@pytest.fixture
def survey_in_database(
    sample_processed_metadata, sample_processed_results, db_connection
):
    """Prepare database with survey data for documentation testing."""
    survey_id = "test_doc_survey"

    # Add load_counter to results
    results_with_counter = sample_processed_results.results_df.with_columns(
        pl.lit(0).alias("load_counter")
    )

    # Save to database
    save_to_database(
        results_df=results_with_counter,
        metadata_df=sample_processed_metadata.final_metadata_df,
        table_prefix=survey_id,
        append=False,
    )

    return survey_id


def test_generate_doc_from_database(survey_in_database, real_config, db_connection):
    """Test complete documentation generation from database."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create config with temporary doc directory
        test_config = Config(
            survey_id=survey_in_database,
            lp_poll_id=real_config.lp_poll_id,
            lp_api_token=real_config.lp_api_token,
            lp_api_base_url=real_config.lp_api_base_url,
            llm_model=real_config.llm_model,
            llm_key=real_config.llm_key,
            llm_base_url=real_config.llm_base_url,
            doc_output_dir=str(Path(tmpdir) / "docs"),
        )

        # Generate documentation from database
        documented_state = generate_doc(test_config)

        # Verify markdown files were created
        docs_dir = documented_state.docs_dir
        assert (docs_dir / "survey_documentation.md").exists()
        assert (docs_dir / "index.md").exists()

        # Verify content
        with open(docs_dir / "survey_documentation.md", "r", encoding="utf-8") as f:
            content = f.read()
            assert "# Survey Documentation" in content
            assert "Total questions:" in content
            assert "Total variables:" in content

        # Verify MkDocs site was built
        assert documented_state.site_dir.exists()
        assert (documented_state.site_dir / "index.html").exists()


def test_generate_doc_with_custom_mkdocs_config(
    survey_in_database, real_config, db_connection
):
    """Test documentation generation with custom MkDocs settings."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create config with custom MkDocs settings
        test_config = Config(
            survey_id=survey_in_database,
            lp_poll_id=real_config.lp_poll_id,
            lp_api_token=real_config.lp_api_token,
            lp_api_base_url=real_config.lp_api_base_url,
            llm_model=real_config.llm_model,
            llm_key=real_config.llm_key,
            llm_base_url=real_config.llm_base_url,
            doc_output_dir=str(Path(tmpdir) / "docs"),
            mkdocs_site_name="Custom Survey Name",
            mkdocs_site_author="Test Author",
            mkdocs_repo_url="https://github.com/test/repo",
        )

        # Generate documentation
        documented_state = generate_doc(test_config)

        # Verify site was built successfully
        assert documented_state.site_dir.exists()
        assert (documented_state.site_dir / "index.html").exists()

        # Verify site includes custom configuration
        # MkDocs embeds site_name in the HTML
        with open(documented_state.site_dir / "index.html", "r", encoding="utf-8") as f:
            html_content = f.read()
            assert "Custom Survey Name" in html_content


def test_generate_doc_missing_metadata_fails(real_config, db_connection):
    """Test that generate_doc fails gracefully when metadata table doesn't exist."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create config for non-existent survey
        test_config = Config(
            survey_id="nonexistent_survey",
            lp_poll_id=real_config.lp_poll_id,
            lp_api_token=real_config.lp_api_token,
            lp_api_base_url=real_config.lp_api_base_url,
            llm_model=real_config.llm_model,
            llm_key=real_config.llm_key,
            llm_base_url=real_config.llm_base_url,
            doc_output_dir=str(Path(tmpdir) / "docs"),
        )

        # Should raise exception when metadata table doesn't exist
        with pytest.raises(Exception):
            generate_doc(test_config)
