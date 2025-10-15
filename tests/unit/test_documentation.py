import os
import tempfile
from pathlib import Path

from plumberlama import Config
from plumberlama.documentation import (
    create_documentation_dataframe,
    create_markdown_files,
)
from plumberlama.transitions import generate_doc


def test_generate_doc_creates_markdown_files(
    sample_parsed_metadata,
    sample_processed_metadata,
    sample_processed_results,
    real_config,
):
    """Test that markdown files are created from documentation DataFrame."""

    # Create temporary directory for docs
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create documentation DataFrame (metadata_df already has everything)
        doc_df = create_documentation_dataframe(
            sample_processed_metadata.final_metadata_df
        )

        # Create markdown files
        num_questions = sample_processed_metadata.final_metadata_df[
            "question_id"
        ].n_unique()
        create_markdown_files(doc_df, num_questions, tmpdir, "test_survey")

        # Verify markdown files were created
        assert os.path.exists(
            os.path.join(tmpdir, "survey_documentation.md")
        ), "survey_documentation.md should be created"
        assert os.path.exists(
            os.path.join(tmpdir, "index.md")
        ), "index.md should be created"

        # Verify content in survey_documentation.md
        with open(
            os.path.join(tmpdir, "survey_documentation.md"), "r", encoding="utf-8"
        ) as f:
            content = f.read()
            assert "# Survey Documentation" in content
            assert "Total questions:" in content
            assert "Total variables:" in content

        # Verify DataFrame was returned
        assert doc_df is not None
        assert len(doc_df) > 0


def test_generate_doc_with_mkdocs_build(
    sample_parsed_metadata,
    sample_processed_metadata,
    sample_processed_results,
    real_config,
    db_connection,
):
    """Test that generate_doc creates MkDocs site from database."""
    import polars as pl

    from plumberlama.io.database import save_to_database

    # Create temporary directory structure
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        docs_dir = tmpdir_path / "docs"
        docs_dir.mkdir()

        # Save metadata to database for testing
        survey_id = "test_mkdocs_build"
        results_with_counter = sample_processed_results.results_df.with_columns(
            pl.lit(0).alias("load_counter")
        )
        save_to_database(
            results_df=results_with_counter,
            metadata_df=sample_processed_metadata.final_metadata_df,
            table_prefix=survey_id,
            append=False,
        )

        # Create config with temporary doc directory
        test_config = Config(
            survey_id=survey_id,
            lp_poll_id=real_config.lp_poll_id,
            lp_api_token=real_config.lp_api_token,
            lp_api_base_url=real_config.lp_api_base_url,
            llm_model=real_config.llm_model,
            llm_key=real_config.llm_key,
            llm_base_url=real_config.llm_base_url,
            doc_output_dir=str(docs_dir),
        )

        # Generate documentation using pipeline function
        documented_state = generate_doc(test_config)

        # Verify markdown files were created
        assert (docs_dir / "survey_documentation.md").exists()
        assert (docs_dir / "index.md").exists()

        # Verify MkDocs site was built
        site_dir = tmpdir_path / "site"
        assert site_dir.exists(), "MkDocs site directory should be created"
        assert (site_dir / "index.html").exists(), "index.html should be generated"
        assert (
            site_dir / "survey_documentation"
        ).exists(), "survey_documentation page should be generated"

        # Verify DocumentedState was returned
        assert documented_state.docs_dir == docs_dir
        assert documented_state.site_dir == site_dir


def test_generate_doc_creates_mkdocs_yml(
    sample_parsed_metadata,
    sample_processed_metadata,
    sample_processed_results,
    real_config,
    db_connection,
):
    """Test that documentation builds without creating mkdocs.yml file (uses in-memory config)."""
    import polars as pl

    from plumberlama.io.database import save_to_database

    # Create temporary directory structure
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        docs_dir = tmpdir_path / "docs"
        docs_dir.mkdir()

        # Save metadata to database for testing
        survey_id = "test_mkdocs_yml"
        results_with_counter = sample_processed_results.results_df.with_columns(
            pl.lit(0).alias("load_counter")
        )
        save_to_database(
            results_df=results_with_counter,
            metadata_df=sample_processed_metadata.final_metadata_df,
            table_prefix=survey_id,
            append=False,
        )

        # Create config with custom MkDocs settings
        test_config = Config(
            survey_id=survey_id,
            lp_poll_id=real_config.lp_poll_id,
            lp_api_token=real_config.lp_api_token,
            lp_api_base_url=real_config.lp_api_base_url,
            llm_model=real_config.llm_model,
            llm_key=real_config.llm_key,
            llm_base_url=real_config.llm_base_url,
            doc_output_dir=str(docs_dir),
            mkdocs_site_name="Test Survey",
            mkdocs_site_author="Test Author",
            mkdocs_repo_url="https://github.com/test/repo",
        )

        # Generate documentation using pipeline function
        documented_state = generate_doc(test_config)

        # Verify mkdocs.yml was NOT created (config is in-memory only)
        mkdocs_yml = tmpdir_path / "mkdocs.yml"
        assert (
            not mkdocs_yml.exists()
        ), "mkdocs.yml should not be created (using in-memory config)"

        # Verify MkDocs site was built successfully
        site_dir = tmpdir_path / "site"
        assert site_dir.exists(), "MkDocs site directory should be created"
        assert (site_dir / "index.html").exists(), "index.html should be generated"

        # Verify DocumentedState was returned
        assert documented_state.docs_dir == docs_dir
        assert documented_state.site_dir == site_dir
