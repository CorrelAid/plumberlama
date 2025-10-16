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
from plumberlama.transitions import TableNotFoundError, generate_doc


@pytest.fixture
def survey_in_database(
    sample_processed_metadata,
    sample_processed_results,
    test_config_for_docs,
    db_connection,
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
        config=test_config_for_docs,
    )

    return survey_id


@pytest.fixture
def test_config_for_docs(real_config):
    """Create test config with test database settings for documentation tests."""
    return Config(
        survey_id=real_config.survey_id,
        lp_poll_id=real_config.lp_poll_id,
        lp_api_token=real_config.lp_api_token,
        lp_api_base_url=real_config.lp_api_base_url,
        llm_model=real_config.llm_model,
        llm_key=real_config.llm_key,
        llm_base_url=real_config.llm_base_url,
        site_output_dir=real_config.site_output_dir,
        mkdocs_site_name=real_config.mkdocs_site_name,
        mkdocs_site_author=real_config.mkdocs_site_author,
        mkdocs_repo_url=real_config.mkdocs_repo_url,
        mkdocs_logo_url=real_config.mkdocs_logo_url,
        db_host="localhost",
        db_port=5433,
        db_name="test_db",
        db_user="test_user",
        db_password="test_password",
    )


def test_generate_doc_from_database(
    survey_in_database, test_config_for_docs, db_connection
):
    """Test complete documentation generation from database."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Update config with survey ID and temporary doc directory
        test_config = Config(
            survey_id=survey_in_database,
            lp_poll_id=test_config_for_docs.lp_poll_id,
            lp_api_token=test_config_for_docs.lp_api_token,
            lp_api_base_url=test_config_for_docs.lp_api_base_url,
            llm_model=test_config_for_docs.llm_model,
            llm_key=test_config_for_docs.llm_key,
            llm_base_url=test_config_for_docs.llm_base_url,
            site_output_dir=str(Path(tmpdir) / "docs"),
            mkdocs_site_name=test_config_for_docs.mkdocs_site_name,
            mkdocs_site_author=test_config_for_docs.mkdocs_site_author,
            mkdocs_repo_url=test_config_for_docs.mkdocs_repo_url,
            mkdocs_logo_url=test_config_for_docs.mkdocs_logo_url,
            db_host=test_config_for_docs.db_host,
            db_port=test_config_for_docs.db_port,
            db_name=test_config_for_docs.db_name,
            db_user=test_config_for_docs.db_user,
            db_password=test_config_for_docs.db_password,
        )

        # Generate documentation from database
        documented_state = generate_doc(test_config)

        # Verify MkDocs site was built
        site_dir = documented_state.site_dir
        assert site_dir.exists()
        assert (site_dir / "index.html").exists()
        assert (site_dir / "survey_documentation" / "index.html").exists()

        # Verify content in built HTML
        with open(site_dir / "index.html", "r", encoding="utf-8") as f:
            content = f.read()
            assert "Survey Documentation" in content


def test_generate_doc_with_custom_mkdocs_config(
    survey_in_database, test_config_for_docs, db_connection
):
    """Test documentation generation with custom MkDocs settings."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Update config with custom MkDocs settings
        test_config = Config(
            survey_id=survey_in_database,
            lp_poll_id=test_config_for_docs.lp_poll_id,
            lp_api_token=test_config_for_docs.lp_api_token,
            lp_api_base_url=test_config_for_docs.lp_api_base_url,
            llm_model=test_config_for_docs.llm_model,
            llm_key=test_config_for_docs.llm_key,
            llm_base_url=test_config_for_docs.llm_base_url,
            site_output_dir=str(Path(tmpdir) / "docs"),
            mkdocs_site_name="Custom Survey Name",
            mkdocs_site_author="Test Author",
            mkdocs_repo_url="https://github.com/test/repo",
            mkdocs_logo_url=test_config_for_docs.mkdocs_logo_url,
            db_host=test_config_for_docs.db_host,
            db_port=test_config_for_docs.db_port,
            db_name=test_config_for_docs.db_name,
            db_user=test_config_for_docs.db_user,
            db_password=test_config_for_docs.db_password,
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


def test_generate_doc_missing_metadata_fails(test_config_for_docs, db_connection):
    """Test that generate_doc fails gracefully when metadata table doesn't exist."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Update config for non-existent survey
        test_config = Config(
            survey_id="nonexistent_survey",
            lp_poll_id=test_config_for_docs.lp_poll_id,
            lp_api_token=test_config_for_docs.lp_api_token,
            lp_api_base_url=test_config_for_docs.lp_api_base_url,
            llm_model=test_config_for_docs.llm_model,
            llm_key=test_config_for_docs.llm_key,
            llm_base_url=test_config_for_docs.llm_base_url,
            site_output_dir=str(Path(tmpdir) / "docs"),
            mkdocs_site_name=test_config_for_docs.mkdocs_site_name,
            mkdocs_site_author=test_config_for_docs.mkdocs_site_author,
            mkdocs_repo_url=test_config_for_docs.mkdocs_repo_url,
            mkdocs_logo_url=test_config_for_docs.mkdocs_logo_url,
            db_host=test_config_for_docs.db_host,
            db_port=test_config_for_docs.db_port,
            db_name=test_config_for_docs.db_name,
            db_user=test_config_for_docs.db_user,
            db_password=test_config_for_docs.db_password,
        )

        # Should raise TableNotFoundError when metadata table doesn't exist
        with pytest.raises(
            TableNotFoundError,
            match="Table 'nonexistent_survey_metadata' does not exist",
        ):
            generate_doc(test_config)
