import os

import polars as pl
import pytest
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, text

from plumberlama.config import Config
from plumberlama.generated_api_models import Questions
from plumberlama.io.api import preprocess_api_response
from plumberlama.logging_config import setup_logging
from plumberlama.states import FetchedMetadataState, ProcessedMetadataState
from plumberlama.validation_schemas import make_results_schema

# Load environment variables from .env file
load_dotenv()

# Initialize logging for tests
setup_logging(level="INFO")


@pytest.fixture
def real_config():
    """Create a real configuration using environment variables for integration tests."""
    # Skip if environment variables are not set
    if not os.getenv("LP_API_TOKEN") or not os.getenv("LP_POLL_ID"):
        pytest.skip(
            "Real API credentials not available (LP_API_TOKEN or LP_POLL_ID not set)"
        )

    return Config(
        lp_poll_id=int(os.getenv("LP_POLL_ID")),
        lp_api_token=os.getenv("LP_API_TOKEN"),
        lp_api_base_url=os.getenv(
            "LP_API_BASE_URL", "https://app.lamapoll.de/assets/api/v2"
        ),
        llm_model=os.getenv("LLM_MODEL", "mistralai/mistral-small-3.2-24b-instruct"),
        llm_key=os.getenv("OR_KEY", "test-key"),
        llm_base_url=os.getenv("LLM_BASE_URL", "https://openrouter.ai/api/v1"),
        site_output_dir=os.getenv("SITE_OUTPUT_DIR", "/tmp/docs"),
        survey_id=os.getenv("SURVEY_ID", "test_survey"),
        mkdocs_site_name=os.getenv("MKDOCS_SITE_NAME", "Test Survey"),
        mkdocs_site_author=os.getenv("MKDOCS_SITE_AUTHOR", "Test Author"),
        mkdocs_repo_url=os.getenv("MKDOCS_REPO_URL", ""),
        mkdocs_logo_url=os.getenv("MKDOCS_LOGO_URL", ""),
        db_host=os.getenv("DB_HOST", "localhost"),
        db_port=int(os.getenv("DB_PORT", "5432")),
        db_name=os.getenv("DB_NAME", "survey_data"),
        db_user=os.getenv("DB_USER", "plumberlama"),
        db_password=os.getenv("DB_PASSWORD", "plumberlama_dev"),
    )


@pytest.fixture
def sample_questions_list():
    """Complete list of questions as returned by LamaPoll API /polls/{id}/questions endpoint.

    Includes various question types:
    - INPUT (single and multiple)
    - CHOICE (single, multiple, multiple with other)
    - MATRIX
    - SCALE
    """
    return [
        {
            "id": 1,
            "pollId": 123,
            "type": "INPUT",
            "question": {"de": "Wie lautet dein Name?"},
            "position": 1,
            "pageId": 100,
            "groups": [
                {
                    "id": 0,
                    "name": {},
                    "varnames": ["V1"],
                    "labels": [],
                    "codes": [],
                    "items": [{"id": "1", "name": {"de": "Name"}}],
                    "inputType": "SINGLELINE",
                }
            ],
        },
        {
            "id": 2,
            "pollId": 123,
            "type": "INPUT",
            "question": {"de": "Wie alt bist du?"},
            "position": 2,
            "pageId": 100,
            "groups": [
                {
                    "id": 0,
                    "name": {},
                    "varnames": ["V2"],
                    "labels": [],
                    "codes": [],
                    "items": [{"id": "1", "name": {"de": "Alter"}}],
                    "inputType": "INTEGER",
                }
            ],
        },
        {
            "id": 3,
            "pollId": 123,
            "type": "INPUT",
            "question": {"de": "Kontaktinformationen"},
            "position": 3,
            "pageId": 100,
            "groups": [
                {
                    "id": 0,
                    "varnames": ["V3"],
                    "name": {"de": "E-Mail"},
                    "labels": [],
                    "codes": [],
                    "items": [{"id": "1", "name": {"de": "E-Mail"}}],
                    "inputType": "SINGLELINE",
                },
                {
                    "id": 1,
                    "varnames": ["V4"],
                    "name": {"de": "Telefon"},
                    "labels": [],
                    "codes": [],
                    "items": [{"id": "2", "name": {"de": "Telefon"}}],
                    "inputType": "SINGLELINE",
                },
            ],
        },
        {
            "id": 4,
            "pollId": 123,
            "type": "CHOICE",
            "question": {"de": "Wie zufrieden bist du?"},
            "position": 1,
            "pageId": 200,
            "groups": [
                {
                    "id": 0,
                    "name": {},
                    "varnames": ["V5"],
                    "labels": [
                        {"de": "Sehr zufrieden"},
                        {"de": "Zufrieden"},
                        {"de": "Neutral"},
                        {"de": "Unzufrieden"},
                    ],
                    "codes": ["1", "2", "3", "4"],
                    "items": [{"id": "1", "name": {}}],
                }
            ],
        },
        {
            "id": 5,
            "pollId": 123,
            "type": "CHOICE",
            "question": {"de": "Welche Farben magst du?"},
            "position": 2,
            "pageId": 200,
            "groups": [
                {
                    "id": 0,
                    "name": {},
                    "varnames": ["V6", "V7", "V8"],
                    "labels": [{"de": "Rot"}, {"de": "Blau"}, {"de": "Grün"}],
                    "codes": [],
                    "items": [],
                }
            ],
        },
        {
            "id": 6,
            "pollId": 123,
            "type": "CHOICE",
            "question": {"de": "Warum engagierst du dich?"},
            "position": 3,
            "pageId": 200,
            "groups": [
                {
                    "id": 0,
                    "name": {},
                    "varnames": ["V9", "V10", "V11"],
                    "labels": [{"de": "Spaß"}, {"de": "Lernen"}, {"de": "Anderes"}],
                    "codes": [],
                    "items": [],
                },
                {
                    "id": 1,
                    "name": {},
                    "varnames": ["V11.1"],
                    "labels": [],
                    "codes": [],
                    "inputType": "SINGLELINE",
                    "items": [{"id": "1", "name": {"de": "Bitte spezifizieren"}}],
                },
            ],
        },
        {
            "id": 7,
            "pollId": 123,
            "type": "MATRIX",
            "question": {"de": "Bewerte die folgenden Aussagen"},
            "position": 1,
            "pageId": 300,
            "groups": [
                {
                    "id": 0,
                    "name": {},
                    "varnames": ["V12", "V13", "V14"],
                    "labels": [
                        {"de": "Stimme zu"},
                        {"de": "Neutral"},
                        {"de": "Stimme nicht zu"},
                    ],
                    "codes": [],
                    "items": [
                        {"id": "1", "name": {"de": "Die Ausbildung war gut"}},
                        {"id": "2", "name": {"de": "Das Team war hilfsbereit"}},
                        {"id": "3", "name": {"de": "Die Inhalte waren relevant"}},
                    ],
                    "range": [1, 3, 1],
                }
            ],
        },
        {
            "id": 8,
            "pollId": 123,
            "type": "SCALE",
            "question": {"de": "Wie wahrscheinlich ist eine Weiterempfehlung?"},
            "position": 2,
            "pageId": 300,
            "groups": [
                {
                    "id": 0,
                    "name": {},
                    "varnames": ["V15"],
                    "labels": [],
                    "codes": [],
                    "items": [],
                    "range": [0, 10, 1],
                }
            ],
        },
    ]


@pytest.fixture
def single_input_question():
    """Single text input question."""
    return {
        "id": 1,
        "pollId": 123,
        "type": "INPUT",
        "question": {"de": "Wie lautet dein Name?"},
        "position": 1,
        "pageId": 100,
        "groups": [
            {
                "id": 0,
                "name": {},
                "varnames": ["V1"],
                "labels": [],
                "codes": [],
                "items": [{"id": "1", "name": {"de": "Name"}}],
                "inputType": "SINGLELINE",
            }
        ],
    }


@pytest.fixture
def single_choice_question():
    """Single choice question with codes."""
    return {
        "id": 4,
        "pollId": 123,
        "type": "CHOICE",
        "question": {"de": "Wie zufrieden bist du?"},
        "position": 1,
        "pageId": 200,
        "groups": [
            {
                "id": 0,
                "name": {},
                "varnames": ["V5"],
                "labels": [
                    {"de": "Sehr zufrieden"},
                    {"de": "Zufrieden"},
                    {"de": "Neutral"},
                    {"de": "Unzufrieden"},
                ],
                "codes": ["1", "2", "3", "4"],
                "items": [{"id": "1", "name": {}}],
            }
        ],
    }


@pytest.fixture
def multiple_choice_question():
    """Multiple choice question."""
    return {
        "id": 5,
        "pollId": 123,
        "type": "CHOICE",
        "question": {"de": "Welche Farben magst du?"},
        "position": 2,
        "pageId": 200,
        "groups": [
            {
                "id": 0,
                "name": {},
                "varnames": ["V6", "V7", "V8"],
                "labels": [{"de": "Rot"}, {"de": "Blau"}, {"de": "Grün"}],
                "codes": [],
                "items": [],
            }
        ],
    }


@pytest.fixture
def matrix_question():
    """Matrix question with items and labels."""
    return {
        "id": 7,
        "pollId": 123,
        "type": "MATRIX",
        "question": {"de": "Bewerte die folgenden Aussagen"},
        "position": 1,
        "pageId": 300,
        "groups": [
            {
                "id": 0,
                "name": {},
                "varnames": ["V12", "V13", "V14"],
                "labels": [
                    {"de": "Stimme zu"},
                    {"de": "Neutral"},
                    {"de": "Stimme nicht zu"},
                ],
                "codes": [],
                "items": [
                    {"id": "1", "name": {"de": "Die Ausbildung war gut"}},
                    {"id": "2", "name": {"de": "Das Team war hilfsbereit"}},
                    {"id": "3", "name": {"de": "Die Inhalte waren relevant"}},
                ],
                "range": [1, 3, 1],
            }
        ],
    }


@pytest.fixture
def sample_loaded_metadata(sample_questions_list):
    """Create a FetchedMetadataState from sample questions data.

    This fixture provides a mock FetchedMetadataState for testing the extract_poll_metadata
    function without requiring a real API call.

    Returns:
        FetchedMetadataState with validated Pydantic Questions objects
    """
    # Preprocess the sample data (same as fetch_poll_metadata does)
    preprocessed = preprocess_api_response(sample_questions_list)

    # Validate with Pydantic models
    questions = [Questions(**q) for q in preprocessed]

    return FetchedMetadataState(raw_questions=questions)


@pytest.fixture
def sample_parsed_metadata(sample_loaded_metadata):
    """Create a ParsedMetadataState from sample loaded metadata.

    Returns:
        ParsedMetadataState with question and variable DataFrames
    """
    from plumberlama.transitions import parse_poll_metadata

    return parse_poll_metadata(sample_loaded_metadata)


@pytest.fixture
def llm_and_generator(real_config):
    """Load LLM and generator for variable naming tests.

    Returns:
        Tuple of (llm, generator)
    """
    from plumberlama.transform.llm import load_llm, make_generator

    llm = load_llm(real_config.llm_model, real_config.llm_key, real_config.llm_base_url)
    generator = make_generator()
    return llm, generator


@pytest.fixture
def sample_processed_metadata(sample_parsed_metadata):
    """Create a ProcessedMetadataState with hardcoded variable names.

    This fixture does NOT call the LLM - it uses pre-determined variable names
    for testing purposes, avoiding expensive LLM calls during tests that don't
    need to test the variable naming logic itself.

    Returns:
        ProcessedMetadataState with renamed variables and schema
    """

    # Create a simple rename mapping based on question labels
    # This mimics what the LLM would do but uses deterministic names
    rename_mapping = {
        "V1": "name",
        "V2": "age",
        "V3": "email",
        "V4": "phone",
        "V5": "satisfaction",
        "V6": "color_red",
        "V7": "color_blue",
        "V8": "color_green",
        "V9": "motivation_fun",
        "V10": "motivation_learning",
        "V11": "motivation_other",
        "V11.1": "motivation_other_text",
        "V12": "training_quality",
        "V13": "team_helpful",
        "V14": "content_relevant",
        "V15": "recommendation_likelihood",
    }

    # Store original ID and rename the variable IDs in the metadata DataFrame
    final_metadata_df = sample_parsed_metadata.parsed_metadata_df.with_columns(
        pl.col("id").alias("original_id")
    ).with_columns(
        pl.col("id").replace_strict(
            rename_mapping, default=pl.col("id"), return_dtype=pl.String
        )
    )

    # Create schema
    processed_results_schema = make_results_schema(final_metadata_df)

    return ProcessedMetadataState(
        final_metadata_df=final_metadata_df,
        processed_results_schema=processed_results_schema,
    )


@pytest.fixture
def sample_loaded_results(sample_processed_metadata):
    """Create a FetchedResultsState with mock results data.

    This fixture does NOT call the API - it uses mock results data that matches
    the sample questions structure, avoiding API calls during tests.

    Returns:
        FetchedResultsState with mock results and schema
    """

    import polars as pl

    from plumberlama.states import FetchedResultsState

    # Create mock results data that matches the sample questions structure
    # This represents what the API would return as CSV strings
    mock_results = {
        "vID": ["1", "2", "3"],
        "vCOMPLETED": ["1", "1", "1"],
        "vFINISHED": ["1", "1", "0"],
        "vDURATION": ["120.5", "95.3", "45.2"],
        "vQUOTE": ["Q1", "Q2", "Q3"],
        "vSTART": ["2024-01-01 10:00:00", "2024-01-01 11:00:00", "2024-01-01 12:00:00"],
        "vEND": ["2024-01-01 10:02:00", "2024-01-01 11:01:00", "2024-01-01 12:00:45"],
        "vRUNTIME": ["120s", "95s", "45s"],
        "vPAGETIME1": ["30", "25", "15"],
        "vPAGETIME2": ["50", "40", "20"],
        "vPAGETIME3": ["40", "30", "10"],
        "vDATE": ["2024-01-01", "2024-01-01", "2024-01-01"],
        "vANONYM": ["0", "0", "0"],
        "vLANG": ["de", "de", "de"],
        # Sample question responses
        "V1": ["Alice", "Bob", "Charlie"],  # name (text)
        "V2": ["25", "30", "35"],  # age (integer)
        "V3": ["alice@test.com", "bob@test.com", "charlie@test.com"],  # email
        "V4": ["123456", "234567", "345678"],  # phone
        "V5": ["1", "2", "3"],  # satisfaction (single choice: codes 1-4)
        "V6": ["1", "0", "1"],  # color_red (multiple choice boolean)
        "V7": ["0", "1", "0"],  # color_blue
        "V8": ["1", "1", "0"],  # color_green
        "V9": ["1", "0", "1"],  # motivation_fun
        "V10": ["1", "1", "0"],  # motivation_learning
        "V11": ["0", "0", "1"],  # motivation_other
        "V11.1": ["", "", "Other reason"],  # motivation_other_text
        "V12": ["1", "2", "3"],  # training_quality (matrix with range 1-3)
        "V13": ["2", "3", "1"],  # team_helpful
        "V14": ["3", "2", "2"],  # content_relevant
        "V15": ["8", "9", "7"],  # recommendation_likelihood (scale 0-10)
    }

    raw_results_df = pl.DataFrame(mock_results)

    return FetchedResultsState(raw_results_df=raw_results_df)


@pytest.fixture
def sample_processed_results(sample_processed_metadata, sample_loaded_results):
    """Create a ProcessedResultsState from sample processed metadata and loaded results.

    This fixture does NOT call the API - it uses mock data that has been processed
    through the pipeline transformations.

    Returns:
        ProcessedResultsState with processed and validated results
    """
    from plumberlama.transitions import process_poll_results

    return process_poll_results(sample_processed_metadata, sample_loaded_results)


@pytest.fixture
def variable_naming_test_data(sample_parsed_metadata):
    """Extract metadata DataFrame for variable naming tests.

    Derived from sample_parsed_metadata following state flow.

    Returns:
        Metadata DataFrame with question text already joined
    """
    return sample_parsed_metadata.parsed_metadata_df


@pytest.fixture
def single_variable_subset(variable_naming_test_data):
    """Subset for testing single variable renaming (Q1).

    Returns:
        Metadata DataFrame filtered to question 1
    """
    import polars as pl

    return variable_naming_test_data.filter(pl.col("question_id") == 1)


@pytest.fixture
def multiple_choice_subset(variable_naming_test_data):
    """Subset for testing multiple choice variables (Q5).

    Returns:
        Metadata DataFrame filtered to question 5
    """
    import polars as pl

    return variable_naming_test_data.filter(pl.col("question_id") == 5)


@pytest.fixture
def multiple_choice_other_subset(variable_naming_test_data):
    """Subset for testing multiple choice with 'other' option (Q6).

    Returns:
        Metadata DataFrame filtered to question 6
    """
    import polars as pl

    return variable_naming_test_data.filter(pl.col("question_id") == 6)


@pytest.fixture(scope="session")
def docker_compose_test_db(request):
    """Start PostgreSQL container from docker-compose.test.yml for the test session.

    Starts the container once at the beginning of the test session and
    stops it at the end. Uses subprocess to manage docker-compose directly.

    Features:
    - Supports both 'docker compose' (modern) and 'docker-compose' (legacy)
    - Waits for PostgreSQL to be ready before yielding
    - Provides detailed error messages if startup fails
    - Registers a finalizer to ensure cleanup happens even on test interruption
    - Removes volumes on teardown for a clean state between test runs
    """
    import subprocess
    import time

    compose_file = os.path.join(os.path.dirname(__file__), "docker-compose.test.yml")
    project_name = "plumberlama_test"

    # Try modern docker compose first, fall back to docker-compose
    docker_cmd = ["docker", "compose"]
    try:
        subprocess.run(
            docker_cmd + ["version"],
            check=True,
            capture_output=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        docker_cmd = ["docker-compose"]
        # Verify docker-compose is available
        try:
            subprocess.run(
                docker_cmd + ["version"],
                check=True,
                capture_output=True,
            )
        except (subprocess.CalledProcessError, FileNotFoundError):
            pytest.fail(
                "Neither 'docker compose' nor 'docker-compose' command is available. "
                "Please install Docker and Docker Compose to run integration tests."
            )

    # Start docker compose
    try:
        result = subprocess.run(
            docker_cmd + ["-f", compose_file, "-p", project_name, "up", "-d"],
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as e:
        pytest.fail(
            f"Failed to start test database container:\n"
            f"Command: {' '.join(e.cmd)}\n"
            f"Exit code: {e.returncode}\n"
            f"stdout: {e.stdout}\n"
            f"stderr: {e.stderr}"
        )

    # Wait for PostgreSQL to be ready
    max_attempts = 30
    for attempt in range(max_attempts):
        try:
            result = subprocess.run(
                docker_cmd
                + [
                    "-f",
                    compose_file,
                    "-p",
                    project_name,
                    "exec",
                    "-T",
                    "postgres",
                    "pg_isready",
                    "-U",
                    "test_user",
                ],
                capture_output=True,
                timeout=5,
                text=True,
            )
            if result.returncode == 0:
                break
        except subprocess.TimeoutExpired:
            pass
        time.sleep(1)
    else:
        # Get container logs for debugging
        logs_result = subprocess.run(
            docker_cmd + ["-f", compose_file, "-p", project_name, "logs", "postgres"],
            capture_output=True,
            text=True,
        )
        # Cleanup on failure
        subprocess.run(
            docker_cmd + ["-f", compose_file, "-p", project_name, "down", "-v"],
            capture_output=True,
        )
        pytest.fail(
            f"PostgreSQL container failed to become ready after {max_attempts} attempts.\n"
            f"Container logs:\n{logs_result.stdout}\n{logs_result.stderr}"
        )

    # Register finalizer to ensure cleanup even if tests are interrupted
    def cleanup():
        print("\n[Teardown] Stopping test database container...")
        try:
            result = subprocess.run(
                docker_cmd + ["-f", compose_file, "-p", project_name, "down", "-v"],
                capture_output=True,
                text=True,
                timeout=30,
            )
            if result.returncode != 0:
                print(
                    f"[Teardown] Warning: Container cleanup had issues:\n{result.stderr}"
                )
            else:
                print("[Teardown] Test database container stopped successfully")
        except subprocess.TimeoutExpired:
            print("[Teardown] Warning: Container cleanup timed out")
            # Force remove containers
            subprocess.run(
                docker_cmd
                + [
                    "-f",
                    compose_file,
                    "-p",
                    project_name,
                    "down",
                    "-v",
                    "--remove-orphans",
                ],
                capture_output=True,
            )
        except Exception as e:
            print(f"[Teardown] Warning: Error during cleanup: {e}")

    request.addfinalizer(cleanup)

    yield


@pytest.fixture
def test_db_config(real_config):
    """Create test config with test database settings for preload and database tests."""
    return Config(
        survey_id="test_survey",
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


@pytest.fixture
def db_connection(docker_compose_test_db, monkeypatch):
    """Create a database connection for integration tests and clean up after.

    Uses PostgreSQL container from docker-compose.test.yml.
    Sets environment variables to ensure all database functions use test DB.

    Yields:
        SQLAlchemy engine connected to test database
    """
    # Set environment variables to point to test database
    monkeypatch.setenv("DB_HOST", "localhost")
    monkeypatch.setenv("DB_PORT", "5433")
    monkeypatch.setenv("DB_USER", "test_user")
    monkeypatch.setenv("DB_PASSWORD", "test_password")
    monkeypatch.setenv("DB_NAME", "test_db")

    # Override connection URI to use test container
    connection_uri = "postgresql://test_user:test_password@localhost:5433/test_db"
    engine = create_engine(connection_uri)

    # Verify connection works
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))

    yield engine

    # Cleanup: drop all test tables after tests
    with engine.connect() as conn:
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        test_tables = [t for t in tables if t.startswith("test_")]
        for table in test_tables:
            conn.execute(text(f"DROP TABLE IF EXISTS {table} CASCADE"))
        conn.commit()
