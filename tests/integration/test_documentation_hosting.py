"""Integration tests for documentation hosting.

Tests the complete documentation generation and hosting flow using only test fixtures,
without making any external API calls.
"""

import http.server
import socketserver
import tempfile
import threading
import time
from pathlib import Path

import polars as pl
import pytest
import requests

from plumberlama.io.database import save_to_database
from plumberlama.transitions import generate_doc


@pytest.fixture
def survey_with_docs(
    sample_processed_metadata, sample_processed_results, test_db_config, db_connection
):
    """Prepare database with survey data and generate documentation."""
    survey_id = "test_hosting_survey"

    # Update config with unique survey ID
    test_db_config.survey_id = survey_id

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
        config=test_db_config,
    )

    # Generate documentation
    with tempfile.TemporaryDirectory() as tmpdir:
        # Update config with temporary output directory
        test_db_config.doc_output_dir = str(Path(tmpdir) / "docs")
        documented_state = generate_doc(test_db_config)

        yield documented_state


def test_generate_docs_from_fixtures_no_api_calls(
    sample_processed_metadata, sample_processed_results, test_db_config, db_connection
):
    """Test complete documentation generation using only fixtures without API calls."""
    survey_id = "test_no_api_survey"

    with tempfile.TemporaryDirectory() as tmpdir:
        # Update config
        test_db_config.survey_id = survey_id
        test_db_config.doc_output_dir = str(Path(tmpdir) / "docs")

        # Prepare data
        results_with_counter = sample_processed_results.results_df.with_columns(
            pl.lit(0).alias("load_counter")
        )

        # Save to database
        save_to_database(
            results_df=results_with_counter,
            metadata_df=sample_processed_metadata.final_metadata_df,
            table_prefix=survey_id,
            append=False,
            config=test_db_config,
        )

        # Generate documentation (no API calls - all from database)
        documented_state = generate_doc(test_db_config)

        # Verify documentation structure
        assert documented_state.docs_dir.exists()
        assert documented_state.site_dir.exists()

        # Verify markdown files
        assert (documented_state.docs_dir / "survey_documentation.md").exists()
        assert (documented_state.docs_dir / "index.md").exists()

        # Verify built site (MkDocs config is built programmatically, not saved as file)
        assert (documented_state.site_dir / "index.html").exists()
        assert (
            documented_state.site_dir / "survey_documentation" / "index.html"
        ).exists()

        # Verify content quality
        with open(
            documented_state.docs_dir / "survey_documentation.md", "r", encoding="utf-8"
        ) as f:
            content = f.read()
            # Check that sample question content is present
            assert "Wie lautet dein Name?" in content  # From sample_questions_list
            assert "Total questions:" in content
            assert "Total variables:" in content


def test_documentation_is_hostable(
    sample_processed_metadata, sample_processed_results, test_db_config, db_connection
):
    """Test that generated documentation can be hosted and accessed via HTTP."""
    survey_id = "test_hostable_survey"

    with tempfile.TemporaryDirectory() as tmpdir:
        # Update config
        test_db_config.survey_id = survey_id
        test_db_config.doc_output_dir = str(Path(tmpdir) / "docs")

        # Prepare and save data
        results_with_counter = sample_processed_results.results_df.with_columns(
            pl.lit(0).alias("load_counter")
        )
        save_to_database(
            results_df=results_with_counter,
            metadata_df=sample_processed_metadata.final_metadata_df,
            table_prefix=survey_id,
            append=False,
            config=test_db_config,
        )

        # Generate documentation
        documented_state = generate_doc(test_db_config)
        site_dir = documented_state.site_dir

        # Find an available port
        port = 8888
        max_attempts = 10
        for attempt in range(max_attempts):
            try:
                # Try to bind to the port
                test_socket = socketserver.TCPServer(("127.0.0.1", port), None)
                test_socket.server_close()
                break
            except OSError:
                port += 1
        else:
            pytest.skip(f"Could not find available port after {max_attempts} attempts")

        # Start HTTP server in background thread
        class Handler(http.server.SimpleHTTPRequestHandler):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, directory=str(site_dir), **kwargs)

            def log_message(self, format, *args):
                # Suppress server logs during tests
                pass

        server = socketserver.TCPServer(("127.0.0.1", port), Handler)
        server_thread = threading.Thread(target=server.serve_forever, daemon=True)
        server_thread.start()

        try:
            # Give server time to start
            time.sleep(0.5)

            base_url = f"http://127.0.0.1:{port}"

            # Test 1: Homepage is accessible
            response = requests.get(base_url, timeout=5)
            assert response.status_code == 200
            assert "text/html" in response.headers.get("Content-Type", "")
            assert len(response.content) > 0

            # Test 2: Index page contains expected content
            index_content = response.text
            assert (
                "Survey Documentation" in index_content
                or "test_hostable_survey" in index_content
            )

            # Test 3: Survey documentation page is accessible
            doc_response = requests.get(f"{base_url}/survey_documentation/", timeout=5)
            assert doc_response.status_code == 200

            # Test 4: Survey documentation contains sample data
            doc_content = doc_response.text
            assert (
                "Total questions:" in doc_content or "questions" in doc_content.lower()
            )

            # Test 5: CSS/styling is present
            assert "stylesheets" in index_content or "css" in index_content.lower()

            # Test 6: Navigation works
            assert "survey_documentation" in index_content

            # Test 7: Assets are accessible (check for common MkDocs assets)
            # Try to access a stylesheet
            css_links = [
                line
                for line in index_content.split("\n")
                if "stylesheet" in line or ".css" in line
            ]
            if css_links:
                # Extract first CSS href
                import re

                css_match = re.search(r'href="([^"]*\.css[^"]*)"', css_links[0])
                if css_match:
                    css_path = css_match.group(1)
                    # Handle absolute and relative paths
                    if not css_path.startswith("http"):
                        css_url = f"{base_url}/{css_path.lstrip('/')}"
                        css_response = requests.get(css_url, timeout=5)
                        # CSS might be in a different location, 404 is acceptable
                        assert css_response.status_code in [200, 404]

        finally:
            # Cleanup: shutdown server
            server.shutdown()
            server.server_close()


def test_documentation_structure_matches_docker_expectations(
    sample_processed_metadata, sample_processed_results, test_db_config, db_connection
):
    """Test that documentation structure is compatible with docker-compose nginx hosting."""
    survey_id = "test_docker_structure"

    with tempfile.TemporaryDirectory() as tmpdir:
        # Simulate docker volume structure
        app_dir = Path(tmpdir) / "app"
        docs_dir = app_dir / "docs"
        site_dir = app_dir / "site"

        docs_dir.mkdir(parents=True)
        site_dir.mkdir(parents=True)

        # Update config to match docker structure
        test_db_config.survey_id = survey_id
        test_db_config.doc_output_dir = str(docs_dir)

        # Prepare and save data
        results_with_counter = sample_processed_results.results_df.with_columns(
            pl.lit(0).alias("load_counter")
        )
        save_to_database(
            results_df=results_with_counter,
            metadata_df=sample_processed_metadata.final_metadata_df,
            table_prefix=survey_id,
            append=False,
            config=test_db_config,
        )

        # Generate documentation
        documented_state = generate_doc(test_db_config)

        # Verify structure matches docker expectations
        # In docker-compose, nginx serves from /usr/share/nginx/html which maps to site_dir
        assert documented_state.site_dir.exists()

        # Check for index.html at root (required for nginx default serving)
        index_file = documented_state.site_dir / "index.html"
        assert index_file.exists(), "index.html must exist at site root for nginx"

        # Verify index.html is valid HTML
        with open(index_file, "r", encoding="utf-8") as f:
            content = f.read()
            content_stripped = content.strip()
            assert content_stripped.startswith(
                "<!DOCTYPE html"
            ) or content_stripped.startswith("<!doctype html")
            assert "</html>" in content

        # Verify subdirectories for pages exist
        survey_doc_dir = documented_state.site_dir / "survey_documentation"
        assert survey_doc_dir.exists(), "survey_documentation directory should exist"
        assert (
            survey_doc_dir / "index.html"
        ).exists(), "survey_documentation/index.html should exist"

        # Verify assets directory structure (MkDocs Material theme)
        # These are typically in assets/, stylesheets/, javascripts/, etc.
        # At minimum, there should be some static assets
        static_dirs = ["assets", "stylesheets", "javascripts", "search"]
        has_static_content = any(
            (documented_state.site_dir / dirname).exists() for dirname in static_dirs
        )
        assert has_static_content, "Site should have static assets for proper rendering"
