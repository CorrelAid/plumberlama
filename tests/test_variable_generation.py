import polars as pl
import pytest
import os
from lib.label_generation import load_llm, make_generator, rename_vars_with_labels

# Load LLM once for all tests
llm_model = "mistralai/mistral-small-3.2-24b-instruct"
llm_key = os.getenv("OR_KEY")
llm_base_url = "https://openrouter.ai/api/v1"
load_llm(llm_model, llm_key, llm_base_url)

# Create generator once for all tests
generator = make_generator()


def test_single_variable_renamed_simply():
    """Questions with single variable should be renamed to Q<position> format"""
    variable_df = pl.DataFrame({
        "id": ["V1"],
        "question_id": [1],
        "question_type": ["single_choice"],
        "type": [pl.String],
        "question_position": [1],
        "group_id": [0],
        "label": [""]
    })

    results_df = pl.DataFrame({
        "V1": ["Red", "Blue", "Green"]
    })

    question_df = pl.DataFrame({
        "id": [1],
        "text": ["What is your favorite color?"]
    })

    renamed_df, updated_variable_df = rename_vars_with_labels(
        results_df, variable_df, generator, question_df
    )

    # Should be renamed to Q1
    assert "Q1" in renamed_df.columns
    assert "V1" not in renamed_df.columns


def test_multiple_variables_renamed_with_labels():
    """Questions with multiple variables should be renamed with labels"""
    variable_df = pl.DataFrame({
        "id": ["V2_1", "V2_2", "V2_3"],
        "question_id": [2, 2, 2],
        "question_type": ["multiple_choice", "multiple_choice", "multiple_choice"],
        "type": [pl.Boolean, pl.Boolean, pl.Boolean],
        "question_position": [2, 2, 2],
        "group_id": [0, 0, 0],
        "label": [
            "I prefer working in teams and collaborating with others",
            "I like to work independently and focus on individual tasks",
            "I enjoy a mix of both collaborative and independent work"
        ]
    })

    results_df = pl.DataFrame({
        "V2_1": [True, False, True],
        "V2_2": [False, True, False],
        "V2_3": [True, True, False]
    })

    question_df = pl.DataFrame({
        "id": [2],
        "text": ["What is your preferred work style?"]
    })

    renamed_df, updated_variable_df = rename_vars_with_labels(
        results_df, variable_df, generator, question_df
    )

    # Should be renamed with LLM-generated names
    # Check that variables were renamed (exact names depend on LLM output)
    assert "V2_1" not in renamed_df.columns
    assert "V2_2" not in renamed_df.columns
    assert "V2_3" not in renamed_df.columns
    # Check that all variables start with Q2_ (LLM-generated pattern)
    assert all(col.startswith("Q2_") for col in renamed_df.columns)
    assert len(renamed_df.columns) == 3


def test_label_special_characters_cleaned():
    """Labels with special characters and complex text are handled by LLM"""
    variable_df = pl.DataFrame({
        "id": ["V5_1", "V5_2", "V5_3"],
        "question_id": [5, 5, 5],
        "question_type": ["multiple_choice", "multiple_choice", "multiple_choice"],
        "type": [pl.Boolean, pl.Boolean, pl.Boolean],
        "question_position": [5, 5, 5],
        "group_id": [0, 0, 0],
        "label": [
            "Python, R, or Julia (data science/statistical programming)",
            "JavaScript, TypeScript, or Node.js (web development)",
            "C++, Rust, or Go (systems programming & performance)"
        ]
    })

    results_df = pl.DataFrame({
        "V5_1": [True, False],
        "V5_2": [False, True],
        "V5_3": [True, False]
    })

    question_df = pl.DataFrame({
        "id": [5],
        "text": ["Which programming language ecosystems do you primarily work with?"]
    })

    renamed_df, updated_variable_df = rename_vars_with_labels(
        results_df, variable_df, generator, question_df
    )

    # LLM should handle special characters and complex text appropriately
    # Check that variables were renamed (exact names depend on LLM output)
    assert "V5_1" not in renamed_df.columns
    assert "V5_2" not in renamed_df.columns
    assert "V5_3" not in renamed_df.columns
    # Check that all variables start with Q5_ (LLM-generated pattern)
    assert all(col.startswith("Q5_") for col in renamed_df.columns)
    assert len(renamed_df.columns) == 3


def test_multiple_choice_other_suffix_naming():
    """Test that 'other' variables get correct suffixes: _other and _other_text"""
    variable_df = pl.DataFrame({
        "id": ["V42", "V42_1", "V42_2", "V42.1"],
        "question_id": [27, 27, 27, 27],
        "question_type": ["multiple_choice_other", "multiple_choice_other", "multiple_choice_other", "multiple_choice_other"],
        "type": [pl.Boolean, pl.Boolean, pl.Boolean, pl.String],
        "question_position": [27, 27, 27, 27],
        "group_id": [0, 0, 0, 1],
        "label": ["Apfel", "Birne", "Anderes", "Anderes (Text)"],
        "is_other_boolean": [False, False, True, False],
        "is_other_text": [False, False, False, True]
    })

    results_df = pl.DataFrame({
        "V42": [True, False, True],
        "V42_1": [False, True, False],
        "V42_2": [True, False, True],
        "V42.1": ["Kiwi", "", "Mango"]
    })

    question_df = pl.DataFrame({
        "id": [27],
        "text": ["Welche Früchte magst du?"]
    })

    renamed_df, updated_variable_df = rename_vars_with_labels(
        results_df, variable_df, generator, question_df
    )

    # Check that 'other' boolean variable ends with _other
    other_boolean_cols = [col for col in renamed_df.columns if col.endswith("_other") and not col.endswith("_other_text")]
    assert len(other_boolean_cols) == 1, f"Expected exactly one _other column, found: {other_boolean_cols}"

    # Check that 'other' text variable ends with _other_text
    other_text_cols = [col for col in renamed_df.columns if col.endswith("_other_text")]
    assert len(other_text_cols) == 1, f"Expected exactly one _other_text column, found: {other_text_cols}"

    # Both should start with Q27_
    assert other_boolean_cols[0].startswith("Q27_")
    assert other_text_cols[0].startswith("Q27_")

    # Other non-'other' variables should not have these suffixes
    other_cols = [col for col in renamed_df.columns if not col.endswith("_other") and not col.endswith("_other_text")]
    assert len(other_cols) == 2  # V42 and V42_1 (non-other choices)


def test_multi_variable_without_labels_indexed():
    """Multi-variable questions without labels should get indexed names Q<pos>_1, Q<pos>_2, etc."""
    variable_df = pl.DataFrame({
        "id": ["V27.C1", "V28.C1", "V29.C1"],
        "question_id": [27, 27, 27],
        "question_type": ["matrix", "matrix", "matrix"],
        "type": [pl.Int64, pl.Int64, pl.Int64],
        "question_position": [27, 27, 27],
        "group_id": [0, 0, 0],
        "label": ["", "", ""]  # No labels
    })

    results_df = pl.DataFrame({
        "V27.C1": [1, 2, 3],
        "V28.C1": [4, 5, 6],
        "V29.C1": [7, 8, 9]
    })

    question_df = pl.DataFrame({
        "id": [27],
        "text": ["Matrix question with no item labels"]
    })

    renamed_df, updated_variable_df = rename_vars_with_labels(
        results_df, variable_df, generator, question_df
    )

    # Should be renamed to indexed format
    assert "Q27_1" in renamed_df.columns
    assert "Q27_2" in renamed_df.columns
    assert "Q27_3" in renamed_df.columns
    assert "V27.C1" not in renamed_df.columns


def test_duplicate_labels_get_numeric_suffix():
    """Variables with duplicate labels should get _2, _3 suffixes to avoid conflicts"""
    variable_df = pl.DataFrame({
        "id": ["V70", "V71", "V72"],
        "question_id": [16, 16, 16],
        "question_type": ["multiple_choice", "multiple_choice", "multiple_choice"],
        "type": [pl.Boolean, pl.Boolean, pl.Boolean],
        "question_position": [16, 16, 16],
        "group_id": [0, 0, 0],
        "label": ["keine Angabe", "keine Angabe", "keine Angabe"]  # All same label
    })

    results_df = pl.DataFrame({
        "V70": [True, False],
        "V71": [False, True],
        "V72": [True, True]
    })

    question_df = pl.DataFrame({
        "id": [16],
        "text": ["Welche Optionen treffen zu?"]
    })

    renamed_df, updated_variable_df = rename_vars_with_labels(
        results_df, variable_df, generator, question_df
    )

    # All should start with Q16_ and contain keine_angabe
    cols = list(renamed_df.columns)
    assert len(cols) == 3
    assert all(col.startswith("Q16_") for col in cols)

    # Should have numeric suffixes to distinguish duplicates
    # First one gets base name, others get _2, _3
    base_cols = [col for col in cols if col.endswith("_2") or col.endswith("_3")]
    assert len(base_cols) == 2  # Two should have numeric suffixes


def test_variable_suffix_no_numbers():
    """Variable suffixes must never contain numbers"""
    import re

    variable_df = pl.DataFrame({
        "id": ["V3_1", "V3_2"],
        "question_id": [3, 3],
        "question_type": ["multiple_choice", "multiple_choice"],
        "type": [pl.Boolean, pl.Boolean],
        "question_position": [3, 3],
        "group_id": [0, 0],
        "label": ["First option", "Second option"]
    })

    results_df = pl.DataFrame({
        "V3_1": [True, False],
        "V3_2": [False, True]
    })

    question_df = pl.DataFrame({
        "id": [3],
        "text": ["Select your options"]
    })

    renamed_df, updated_variable_df = rename_vars_with_labels(
        results_df, variable_df, generator, question_df
    )

    # Check that no column contains numbers in the suffix (after Q<number>_)
    for col in renamed_df.columns:
        # Extract suffix after Q<number>_
        match = re.match(r'Q\d+_(.*)', col)
        if match:
            suffix = match.group(1)
            # Suffix should not contain any numbers
            assert not re.search(r'\d', suffix), f"Variable suffix '{suffix}' in column '{col}' contains numbers, which is not allowed"


def test_variable_suffix_single_word():
    """Variable suffixes must be a single word (no underscores except for _other or _other_text)"""
    import re

    variable_df = pl.DataFrame({
        "id": ["V4_1", "V4_2", "V4_3"],
        "question_id": [4, 4, 4],
        "question_type": ["multiple_choice", "multiple_choice", "multiple_choice"],
        "type": [pl.Boolean, pl.Boolean, pl.Boolean],
        "question_position": [4, 4, 4],
        "group_id": [0, 0, 0],
        "label": ["Apple", "Banana", "Cherry"]
    })

    results_df = pl.DataFrame({
        "V4_1": [True, False],
        "V4_2": [False, True],
        "V4_3": [True, False]
    })

    question_df = pl.DataFrame({
        "id": [4],
        "text": ["What fruits do you like?"]
    })

    renamed_df, updated_variable_df = rename_vars_with_labels(
        results_df, variable_df, generator, question_df
    )

    # Check that suffixes are single words (only lowercase letters including umlauts)
    for col in renamed_df.columns:
        # Extract suffix after Q<number>_
        match = re.match(r'Q\d+_(.+)', col)
        if match:
            suffix = match.group(1)
            # Allow _other and _other_text as special cases
            if suffix.endswith("_other_text") or suffix.endswith("_other"):
                continue
            # Otherwise, suffix should be a single word (only lowercase letters including umlauts)
            assert re.match(r'^[a-zäöüß]+$', suffix, re.UNICODE), f"Variable suffix '{suffix}' in column '{col}' is not a single word (must contain only lowercase letters including umlauts)"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
