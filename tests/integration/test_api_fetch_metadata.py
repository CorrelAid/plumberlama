import pytest
from requests.exceptions import HTTPError

from plumberlama import Config
from plumberlama.api_models import Questions
from plumberlama.states import FetchedMetadataState
from plumberlama.transitions import fetch_poll_metadata


def test_load_poll_metadata_success(real_config):
    result = fetch_poll_metadata(real_config)

    # Verify state structure
    assert isinstance(result, FetchedMetadataState)
    assert isinstance(result.raw_questions, list)
    assert len(result.raw_questions) > 0, "Should have at least one question"

    # Verify all items are validated Pydantic models
    assert all(isinstance(q, Questions) for q in result.raw_questions)

    # Verify basic data integrity
    for question in result.raw_questions:
        assert question.id > 0
        assert question.pollId == real_config.lp_poll_id
        assert question.pageId is not None


def test_load_poll_metadata_invalid_token(real_config):
    """Test that invalid API token raises HTTPError."""
    invalid_config = Config(
        survey_id=real_config.survey_id,
        lp_poll_id=real_config.lp_poll_id,
        lp_api_token="invalid_token_xyz",
        lp_api_base_url=real_config.lp_api_base_url,
        llm_model=real_config.llm_model,
        llm_key=real_config.llm_key,
        llm_base_url=real_config.llm_base_url,
        doc_output_dir=real_config.doc_output_dir,
    )

    with pytest.raises(HTTPError) as exc_info:
        fetch_poll_metadata(invalid_config)

    # If 404, poll doesn't exist - skip
    if exc_info.value.response.status_code == 404:
        pytest.skip(f"Poll {real_config.lp_poll_id} does not exist, cannot test auth.")

    assert exc_info.value.response.status_code in [
        401,
        403,
    ], "Invalid token should return 401 or 403"


def test_load_poll_metadata_nonexistent_poll(real_config):
    """Test that nonexistent poll ID raises 404 HTTPError."""
    invalid_config = Config(
        survey_id=real_config.survey_id,
        lp_poll_id=999999999,
        lp_api_token=real_config.lp_api_token,
        lp_api_base_url=real_config.lp_api_base_url,
        llm_model=real_config.llm_model,
        llm_key=real_config.llm_key,
        llm_base_url=real_config.llm_base_url,
        doc_output_dir=real_config.doc_output_dir,
    )

    with pytest.raises(HTTPError) as exc_info:
        fetch_poll_metadata(invalid_config)

    assert (
        exc_info.value.response.status_code == 404
    ), "Nonexistent poll should return 404"
