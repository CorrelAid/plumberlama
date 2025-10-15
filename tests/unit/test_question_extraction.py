"""Tests for parse_question function."""

import pytest

from plumberlama.generated_api_models import Questions
from plumberlama.parse_metadata import parse_question


@pytest.fixture
def multiple_choice_other_question():
    """Question with 'other' option (V40, V41, V42 choices + V42.1 text field)"""
    return {
        "id": 27937521,
        "pollId": 123,
        "type": "CHOICE",
        "question": {"de": "Warum engagierst du dich bei [U25]?"},
        "position": 8,
        "pageId": 1,
        "groups": [
            {
                "id": 0,
                "name": {},
                "varnames": ["V40", "V41", "V42"],
                "labels": [{"de": "Reason 1"}, {"de": "Reason 2"}, {"de": "Anderes"}],
                "codes": [],
                "items": [],
            },
            {
                "id": 1,
                "name": {},
                "varnames": ["V42.1"],
                "labels": [],
                "codes": [],
                "inputType": "SINGLELINE",
                "items": [{"id": "1", "name": {"de": "Other text"}}],
            },
        ],
    }


@pytest.fixture
def multiple_choice_other_empty_label_question():
    """Question with empty label for 'other' boolean"""
    return {
        "id": 27937575,
        "pollId": 123,
        "type": "CHOICE",
        "question": {"de": "Test question"},
        "position": 16,
        "pageId": 1,
        "groups": [
            {
                "id": 0,
                "name": {},
                "varnames": ["V69", "V70"],
                "labels": [{"de": "Choice 1"}, {}],
                "codes": [],
                "items": [],
            },
            {
                "id": 1,
                "name": {},
                "varnames": ["V70.1"],
                "labels": [],
                "codes": [],
                "inputType": "SINGLELINE",
                "items": [{"id": "1", "name": {"de": "Other text"}}],
            },
        ],
    }


@pytest.fixture
def matrix_question():
    """Matrix question with item labels"""
    return {
        "id": 27937509,
        "pollId": 123,
        "type": "MATRIX",
        "question": {"de": "Matrix question"},
        "position": 4,
        "pageId": 1,
        "groups": [
            {
                "id": 0,
                "name": {},
                "varnames": ["V10", "V11", "V12"],
                "labels": [{"de": "Option A"}, {"de": "Option B"}],
                "codes": [],
                "items": [
                    {"id": "1", "name": {"de": "Item 1"}},
                    {"id": "2", "name": {"de": "Item 2"}},
                    {"id": "3", "name": {"de": "Item 3"}},
                ],
                "range": [1, 2, 1],
            }
        ],
    }


@pytest.fixture
def input_multiple_question():
    """Multiple input question with group names"""
    return {
        "id": 27937500,
        "pollId": 123,
        "type": "INPUT",
        "question": {"de": "Multiple inputs"},
        "position": 1,
        "pageId": 1,
        "groups": [
            {
                "id": 0,
                "varnames": ["V1"],
                "name": {"de": "First field"},
                "labels": [],
                "codes": [],
                "inputType": "SINGLELINE",
                "items": [{"id": "1"}],
            },
            {
                "id": 1,
                "varnames": ["V2"],
                "name": {"de": "Second field"},
                "labels": [],
                "codes": [],
                "inputType": "SINGLELINE",
                "items": [{"id": "2"}],
            },
        ],
    }


@pytest.fixture
def single_choice_question():
    """Single choice question with possible values"""
    return {
        "id": 27937503,
        "pollId": 123,
        "type": "CHOICE",
        "question": {"de": "Single choice"},
        "position": 2,
        "pageId": 1,
        "groups": [
            {
                "id": 0,
                "name": {},
                "varnames": ["V3"],
                "labels": [{"de": "Option A"}, {"de": "Option B"}, {"de": "Option C"}],
                "codes": ["1", "2", "3"],
                "items": [{"id": "1"}],
            }
        ],
    }


@pytest.fixture
def scale_question():
    """Scale question with range"""
    return {
        "id": 27937506,
        "pollId": 123,
        "type": "SCALE",
        "question": {"de": "Rate this"},
        "position": 5,
        "pageId": 1,
        "groups": [
            {
                "id": 0,
                "name": {},
                "varnames": ["V15"],
                "labels": [],
                "codes": [],
                "range": [1, 5, 1],
                "items": [],
            }
        ],
    }


def test_multiple_choice_other_no_duplicate_boolean(multiple_choice_other_question):
    """Test that multiple_choice_other doesn't create duplicate 'other' boolean variable."""
    question = Questions(**multiple_choice_other_question)
    question_dict, variables = parse_question(
        question, absolute_position=8, page_number=1
    )

    assert question_dict["question_type"] == "multiple_choice_other"
    assert len(variables) == 4

    # Check for V42 (other boolean) - should appear exactly once
    v42_vars = [v for v in variables if v["id"] == "V42"]
    assert (
        len(v42_vars) == 1
    ), f"V42 should appear exactly once, found {len(v42_vars)} times"
    assert v42_vars[0]["is_other_boolean"]
    assert v42_vars[0]["schema_variable_type"] == "Boolean"
    assert v42_vars[0]["label"] == "Anderes"

    # Check for V42.1 (other text field)
    v42_1_vars = [v for v in variables if v["id"] == "V42.1"]
    assert len(v42_1_vars) == 1
    assert v42_1_vars[0]["is_other_text"]
    assert v42_1_vars[0]["schema_variable_type"] == "String"

    # Check regular choice variables have is_other_boolean=False
    v40 = [v for v in variables if v["id"] == "V40"][0]
    assert not v40["is_other_boolean"]


def test_multiple_choice_other_empty_label(multiple_choice_other_empty_label_question):
    """Test multiple_choice_other with empty label for 'other' boolean."""
    question = Questions(**multiple_choice_other_empty_label_question)
    question_dict, variables = parse_question(
        question, absolute_position=16, page_number=1
    )

    assert question_dict["question_type"] == "multiple_choice_other"
    assert len(variables) == 3

    # V70 should appear once with empty label
    v70_vars = [v for v in variables if v["id"] == "V70"]
    assert len(v70_vars) == 1
    assert v70_vars[0]["is_other_boolean"]
    assert v70_vars[0]["label"] == ""


def test_matrix_with_items(matrix_question):
    """Test matrix question with item labels."""
    question = Questions(**matrix_question)
    question_dict, variables = parse_question(
        question, absolute_position=4, page_number=1
    )

    assert question_dict["question_type"] == "matrix"
    assert len(variables) == 3
    assert variables[0]["label"] == "Item 1"
    assert variables[1]["label"] == "Item 2"
    assert variables[2]["label"] == "Item 3"

    # Verify scale labels are stored
    for var in variables:
        assert var["schema_variable_type"] == "Int64"
        assert var["scale_labels"] == ["Option A", "Option B"]
        assert var["range_min"] == 1
        assert var["range_max"] == 2


def test_input_multiple_with_group_names(input_multiple_question):
    """Test input_multiple gets labels from group names."""
    question = Questions(**input_multiple_question)
    question_dict, variables = parse_question(
        question, absolute_position=1, page_number=1
    )

    assert question_dict["question_type"] == "input_multiple_singleline"
    assert len(variables) == 2
    assert variables[0]["label"] == "First field"
    assert variables[1]["label"] == "Second field"
    assert variables[0]["schema_variable_type"] == "String"
    assert variables[1]["schema_variable_type"] == "String"


def test_single_choice_with_possible_values(single_choice_question):
    """Test single_choice creates possible_values mapping."""
    question = Questions(**single_choice_question)
    question_dict, variables = parse_question(
        question, absolute_position=1, page_number=1
    )

    assert question_dict["question_type"] == "single_choice"
    assert len(variables) == 1
    assert variables[0]["possible_values_codes"] == ["1", "2", "3"]
    assert variables[0]["possible_values_labels"] == [
        "Option A",
        "Option B",
        "Option C",
    ]
    assert variables[0]["schema_variable_type"] == "String"


def test_scale_with_range(scale_question):
    """Test scale question extracts range correctly."""
    question = Questions(**scale_question)
    question_dict, variables = parse_question(
        question, absolute_position=1, page_number=1
    )

    assert question_dict["question_type"] == "scale"
    assert len(variables) == 1
    assert variables[0]["range_min"] == 1
    assert variables[0]["range_max"] == 5
    assert variables[0]["schema_variable_type"] == "Int64"
