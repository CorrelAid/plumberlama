"""Tests for get_question_type_and_variables function."""
import polars as pl
import pytest
from lib.helpers import get_question_type_and_variables, make_question_df, make_group_df, make_item_df


def test_multiple_choice_other_no_duplicate_boolean():
    """Test that multiple_choice_other doesn't create duplicate 'other' boolean variable."""
    question_obj = {
        "id": 27937521,
        "type": "CHOICE",
        "question": {"de": "Warum engagierst du dich bei [U25]?"},
        "position": 8,
        "pageId": 1,
        "groups": [
            {
                "id": 0,
                "varnames": ["V40", "V41", "V42"],
                "labels": [{"de": "Reason 1"}, {"de": "Reason 2"}, {"de": "Anderes"}],
                "items": [],
                "range": None
            },
            {
                "id": 1,
                "varnames": ["V42.1"],
                "labels": [],
                "inputType": "SINGLELINE",
                "items": [{"id": 1, "name": {"de": "Other text"}}],
                "range": None
            }
        ]
    }

    questions = [question_obj]
    question_df = make_question_df(questions)
    group_df = make_group_df(questions)
    item_df = make_item_df(questions)

    qrow = question_df.to_dicts()[0]
    question_type, variables = get_question_type_and_variables(qrow, group_df, item_df, question_obj)

    assert question_type == "multiple_choice_other"
    assert len(variables) == 4

    # Check for V42 (other boolean) - should appear exactly once
    v42_vars = [v for v in variables if v["id"] == "V42"]
    assert len(v42_vars) == 1, f"V42 should appear exactly once, found {len(v42_vars)} times"
    assert v42_vars[0]["is_other_boolean"] == True
    assert v42_vars[0]["type"] == pl.Boolean
    assert v42_vars[0]["label"] == "Anderes"

    # Check for V42.1 (other text field)
    v42_1_vars = [v for v in variables if v["id"] == "V42.1"]
    assert len(v42_1_vars) == 1
    assert v42_1_vars[0]["is_other_text"] == True
    assert v42_1_vars[0]["type"] == pl.String

    # Check regular choice variables have is_other_boolean=False
    v40 = [v for v in variables if v["id"] == "V40"][0]
    assert v40["is_other_boolean"] == False


def test_multiple_choice_other_empty_label():
    """Test multiple_choice_other with empty label for 'other' boolean."""
    question_obj = {
        "id": 27937575,
        "type": "CHOICE",
        "question": {"de": "Test question"},
        "position": 16,
        "pageId": 1,
        "groups": [
            {
                "id": 0,
                "varnames": ["V69", "V70"],
                "labels": [{"de": "Choice 1"}, []],
                "items": [],
                "range": None
            },
            {
                "id": 1,
                "varnames": ["V70.1"],
                "labels": [],
                "inputType": "SINGLELINE",
                "items": [{"id": 1, "name": {"de": "Other text"}}],
                "range": None
            }
        ]
    }

    questions = [question_obj]
    question_df = make_question_df(questions)
    group_df = make_group_df(questions)
    item_df = make_item_df(questions)

    qrow = question_df.to_dicts()[0]
    question_type, variables = get_question_type_and_variables(qrow, group_df, item_df, question_obj)

    assert question_type == "multiple_choice_other"
    assert len(variables) == 3

    # V70 should appear once with empty label
    v70_vars = [v for v in variables if v["id"] == "V70"]
    assert len(v70_vars) == 1
    assert v70_vars[0]["is_other_boolean"] == True
    assert v70_vars[0]["label"] == ""


def test_matrix_with_items():
    """Test matrix question with item labels."""
    question_obj = {
        "id": 27937509,
        "type": "MATRIX",
        "question": {"de": "Matrix question"},
        "position": 4,
        "pageId": 1,
        "groups": [{
            "id": 0,
            "varnames": ["V10", "V11", "V12"],
            "labels": [{"de": "Option A"}, {"de": "Option B"}],
            "items": [
                {"id": 1, "name": {"de": "Item 1"}},
                {"id": 2, "name": {"de": "Item 2"}},
                {"id": 3, "name": {"de": "Item 3"}}
            ],
            "range": None
        }]
    }

    questions = [question_obj]
    question_df = make_question_df(questions)
    group_df = make_group_df(questions)
    item_df = make_item_df(questions)

    qrow = question_df.to_dicts()[0]
    question_type, variables = get_question_type_and_variables(qrow, group_df, item_df, question_obj)

    assert question_type == "matrix"
    assert len(variables) == 3
    assert variables[0]["label"] == "Item 1"
    assert variables[1]["label"] == "Item 2"
    assert variables[2]["label"] == "Item 3"

    for var in variables:
        assert var["type"] == pl.Int64


def test_input_multiple_with_group_names():
    """Test input_multiple gets labels from group names."""
    question_obj = {
        "id": 27937500,
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
                "inputType": "SINGLELINE",
                "items": [{"id": 1}],
                "range": None
            },
            {
                "id": 1,
                "varnames": ["V2"],
                "name": {"de": "Second field"},
                "labels": [],
                "inputType": "SINGLELINE",
                "items": [{"id": 2}],
                "range": None
            }
        ]
    }

    questions = [question_obj]
    question_df = make_question_df(questions)
    group_df = make_group_df(questions)
    item_df = make_item_df(questions)

    qrow = question_df.to_dicts()[0]
    question_type, variables = get_question_type_and_variables(qrow, group_df, item_df, question_obj)

    assert question_type == "input_multiple_singleline"
    assert len(variables) == 2
    assert variables[0]["label"] == "First field"
    assert variables[1]["label"] == "Second field"
    assert variables[0]["type"] == pl.String
    assert variables[1]["type"] == pl.String


def test_single_choice_with_possible_values():
    """Test single_choice creates possible_values mapping."""
    question_obj = {
        "id": 27937503,
        "type": "CHOICE",
        "question": {"de": "Single choice"},
        "position": 2,
        "pageId": 1,
        "groups": [{
            "id": 0,
            "varnames": ["V3"],
            "labels": [{"de": "Option A"}, {"de": "Option B"}, {"de": "Option C"}],
            "codes": ["1", "2", "3"],
            "items": [{"id": 1}],
            "range": None
        }]
    }

    questions = [question_obj]
    question_df = make_question_df(questions)
    group_df = make_group_df(questions)
    item_df = make_item_df(questions)

    qrow = question_df.to_dicts()[0]
    question_type, variables = get_question_type_and_variables(qrow, group_df, item_df, question_obj)

    assert question_type == "single_choice"
    assert len(variables) == 1
    assert variables[0]["possible_values"] == {
        "1": "Option A",
        "2": "Option B",
        "3": "Option C"
    }
    assert variables[0]["type"] == pl.String


def test_scale_with_range():
    """Test scale question extracts range correctly."""
    question_obj = {
        "id": 27937506,
        "type": "SCALE",
        "question": {"de": "Rate this"},
        "position": 5,
        "pageId": 1,
        "groups": [{
            "id": 0,
            "varnames": ["V15"],
            "labels": [],
            "range": [1, 5, 1],
            "items": []
        }]
    }

    questions = [question_obj]
    question_df = make_question_df(questions)
    group_df = make_group_df(questions)
    item_df = make_item_df(questions)

    qrow = question_df.to_dicts()[0]
    question_type, variables = get_question_type_and_variables(qrow, group_df, item_df, question_obj)

    assert question_type == "scale"
    assert len(variables) == 1
    assert variables[0]["range_min"] == 1
    assert variables[0]["range_max"] == 5
    assert variables[0]["type"] == pl.Int64
