"""Unit tests for documentation generation functions.

Tests the core documentation functions without requiring database connection.
For integration tests with database, see tests/integration/test_documentation_from_db.py
"""

import os
import tempfile

from plumberlama.documentation import (
    create_documentation_dataframe,
    create_markdown_files,
)


def test_generate_doc_creates_markdown_files(
    sample_processed_metadata,
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
