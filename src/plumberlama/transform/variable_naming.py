import re

import polars as pl


def _sanitize_suffix(suffix: str) -> str:
    """Remove German umlauts and other non-ASCII characters from variable suffix.

    Replaces:
    - ä → ae
    - ö → oe
    - ü → ue
    - ß → ss

    Then removes any remaining non-ASCII characters.

    Args:
        suffix: Variable suffix that may contain umlauts

    Returns:
        ASCII-only suffix
    """
    replacements = {
        "ä": "ae",
        "ö": "oe",
        "ü": "ue",
        "Ä": "ae",
        "Ö": "oe",
        "Ü": "ue",
        "ß": "ss",
    }

    result = suffix
    for umlaut, replacement in replacements.items():
        result = result.replace(umlaut, replacement)

    # Remove any remaining non-ASCII characters
    result = result.encode("ascii", "ignore").decode("ascii")

    return result


def _apply_other_suffix(var_name, is_other_text, is_other_boolean):
    """Apply _other or _other_text suffix to variable name."""
    if is_other_text:
        return (
            var_name if var_name.endswith("_other_text") else f"{var_name}_other_text"
        )
    if is_other_boolean:
        return var_name if var_name.endswith("_other") else f"{var_name}_other"
    return var_name


def _generate_llm_name(
    generator,
    lm,
    question_text,
    question_position,
    variable_text,
    previous_names,
    is_other_text,
    is_other_boolean,
    max_retries=3,
):
    """Generate variable name using LLM with validation."""
    for attempt in range(max_retries):
        prompt_suffix = ""
        if previous_names:
            avoid_names = [
                n.replace("AVOID:", "").split("_", 1)[1] if "_" in n else n
                for n in previous_names
                if n
            ]
            prompt_suffix = f" IMPORTANT: Generate a DIFFERENT suffix than these already used: {', '.join(avoid_names)}."

        # Add instruction to avoid umlauts
        prompt_suffix += " Use only ASCII characters a-z (no umlauts like ä, ö, ü, ß)."

        result = generator(
            previous_variable_names=previous_names,
            question_text=question_text + prompt_suffix,
            variable_text=variable_text,
            lm=lm,
        )

        suffix = result.variable_suffix.strip().lstrip("_")

        # Sanitize to remove any umlauts the LLM might have used anyway
        suffix = _sanitize_suffix(suffix)

        # Validate: only lowercase ASCII letters allowed
        if not re.match(r"^[a-z]+$", suffix):
            if attempt < max_retries - 1:
                continue
            else:
                assert (
                    False
                ), f"LLM generated invalid suffix '{suffix}' for '{variable_text}' after {max_retries} attempts."

        var_name = f"Q{question_position}_{suffix}"
        return _apply_other_suffix(var_name, is_other_text, is_other_boolean)

    return var_name


def _process_multi_variable_question(
    generator,
    lm,
    question_vars,
    question_text,
    question_position,
    all_names,
    rename_mapping,
):
    """Process all variables for a multi-variable question."""
    question_var_names = []

    for var in question_vars:
        label = var.get("label", "")
        question_type = var.get("question_type", "")
        is_other_text = var.get("is_other_text", False)
        is_other_boolean = var.get("is_other_boolean", False)

        if question_type == "multiple_choice_other" and (
            is_other_text or is_other_boolean
        ):
            new_var_id = _apply_other_suffix(
                f"Q{question_position}", is_other_text, is_other_boolean
            )
        elif label:
            assert label.strip(), f"Empty label for variable {var.get('id')}"

            for attempt in range(3):
                new_var_id = _generate_llm_name(
                    generator,
                    lm,
                    question_text,
                    question_position,
                    label,
                    question_var_names,
                    is_other_text,
                    is_other_boolean,
                )

                if new_var_id not in all_names and new_var_id not in question_var_names:
                    break

                if attempt < 2:
                    question_var_names.append(f"AVOID:{new_var_id}")

            assert (
                new_var_id not in all_names and new_var_id not in question_var_names
            ), f"Failed to generate unique name for '{label}'. Got '{new_var_id}' after 3 attempts."
        else:
            raise ValueError(
                f"Variable {var.get('id')} at position {question_position} has no label."
            )

        assert new_var_id not in all_names, f"Duplicate variable name '{new_var_id}'"

        question_var_names.append(new_var_id)
        all_names.add(new_var_id)
        rename_mapping[var["id"]] = new_var_id


def rename_vars_with_labels(metadata_df, generator, lm):
    """Rename variable IDs to Q<position>_<suffix> format using LLM and deterministic naming.

    Args:
        metadata_df: Variable-level DataFrame with question_text column already joined
        generator: LLM generator for variable naming
        lm: Language model instance

    Returns:
        DataFrame with renamed variables and original_id column added
    """
    vars_per_question = metadata_df.group_by("question_id").agg(
        pl.count("id").alias("var_count")
    )

    rename_mapping = {}
    all_names = set()

    variables_by_question = {}
    for var in metadata_df.to_dicts():
        if "label" not in var:
            var["label"] = ""
        question_id = var["question_id"]
        variables_by_question.setdefault(question_id, []).append(var)

    for question_id, question_vars in variables_by_question.items():
        var_count = vars_per_question.filter(pl.col("question_id") == question_id)[
            "var_count"
        ][0]

        if var_count == 1:
            var = question_vars[0]
            new_var_id = _apply_other_suffix(
                f"Q{var['question_position']}",
                var.get("is_other_text", False),
                var.get("is_other_boolean", False),
            )
            rename_mapping[var["id"]] = new_var_id
            all_names.add(new_var_id)
        else:
            # Question text is already in the metadata_df
            question_text = question_vars[0]["question_text"]
            question_position = question_vars[0]["question_position"]
            _process_multi_variable_question(
                generator,
                lm,
                question_vars,
                question_text,
                question_position,
                all_names,
                rename_mapping,
            )

    # Store original ID before renaming
    metadata_df = metadata_df.with_columns(pl.col("id").alias("original_id"))

    # Rename the id column
    metadata_df = metadata_df.with_columns(
        pl.col("id")
        .replace_strict(rename_mapping, default=pl.col("id"), return_dtype=pl.String)
        .alias("id")
    )

    return metadata_df
