import polars as pl

from plumberlama.api_models import Questions


def extract_question_type(
    question: Questions, absolute_position: int, page_number: int
):
    """Determine question type and generate variable metadata from Pydantic Question object.

    Args:
        question: Pydantic Questions object
        absolute_position: Absolute position of question in survey
        page_number: Page number of question

    Returns:
        Tuple of (question_dict, variables_list)
    """
    question_id = question.id
    lp_question_type = question.type.value
    number_groups = len(question.groups) if question.groups else 0
    groups = question.groups if question.groups else []

    # Assert German language field exists (this codebase only supports German surveys)
    assert (
        question.question and "de" in question.question
    ), f"Question {question_id} must have German ('de') text field"

    variables = []

    def make_var(var_id, var_type, group_id=0, **extras):
        """Helper to create variable dict with common fields."""
        return {
            "question_id": question_id,
            "group_id": group_id,
            "id": var_id,
            "question_position": absolute_position,
            "question_type": question_type,
            "schema_variable_type": var_type,
            **extras,
        }

    match (lp_question_type, number_groups):
        case ("INPUT", 1):
            # input_single_<input_type>
            group = groups[0]
            assert len(group.labels) == 0
            assert len(group.varnames) == 1
            assert len(group.items) == 1
            item = group.items[0]
            assert (
                group.inputType.value
                if group.inputType
                else None in ["SINGLELINE", "INTEGER", "MULTILINE"]
            )
            question_type = f"input_single_{group.inputType.value.lower()}"
            var_type = pl.Int64 if group.inputType.value == "INTEGER" else pl.String
            variables.append(make_var(group.varnames[0], var_type))

        case ("INPUT", n) if n > 1:
            # input_multiple_<input_type>
            input_types = []
            for group in groups:
                assert len(group.varnames) == 1
                assert len(group.labels) == 0
                assert len(group.items) == 1
                assert group.name.get("de", "") if group.name else ""
                input_types.append(group.inputType.value if group.inputType else None)
            assert len(list(set(input_types))) == 1
            question_type = f"input_multiple_{input_types[0].lower()}"
            var_type = pl.Int64 if input_types[0] == "INTEGER" else pl.String
            for idx, group in enumerate(groups):
                varname = group.varnames[0]
                label_text = ""
                if group.name:
                    label_text = group.name.get("de", "")

                if not label_text:
                    if group.items:
                        item_name = group.items[0].name
                        label_text = item_name.get("de", "") if item_name else ""

                variables.append(
                    make_var(varname, var_type, group_id=idx, label=label_text)
                )

        case ("MATRIX", 1):
            # matrix
            group = groups[0]
            assert len(group.items) > 1
            assert len(group.varnames) == len(group.items)
            assert len(group.labels) > 1
            question_type = "matrix"

            varnames = group.varnames
            group_items = group.items
            labels = group.labels

            # Extract range information for matrix questions
            if group.range and len(group.range) >= 2:
                range_min = group.range[0]
                range_max = group.range[1]
            elif labels and len(labels) > 0:
                # Assume labels represent 1 to N scale
                range_min = 1
                range_max = len(labels)
            else:
                range_min = None
                range_max = None

            for idx, varname in enumerate(varnames):
                item_text = ""
                if idx < len(group_items):
                    item = group_items[idx]
                    if item.name:
                        item_text = item.name.get("de", "")

                variables.append(
                    make_var(
                        varname,
                        pl.Int64,
                        label=item_text,
                        range_min=range_min,
                        range_max=range_max,
                    )
                )

        case ("CHOICE", 1):
            group = groups[0]
            match len(group.varnames):
                case 1:
                    # single_choice
                    assert len(group.items) == 1
                    question_type = "single_choice"
                    codes = group.codes
                    labels = group.labels
                    possible_values = {}
                    if labels:
                        # If codes are empty/missing, generate them as 1, 2, 3, ...
                        if not codes or all(not c or not c.strip() for c in codes):
                            codes = [str(i + 1) for i in range(len(labels))]

                        if len(codes) == len(labels):
                            for code, label in zip(codes, labels):
                                if code and code.strip():
                                    label_text = label.get("de", "") if label else ""
                                    if label_text:
                                        possible_values[code] = label_text

                    variables.append(
                        make_var(
                            group.varnames[0],
                            pl.String,
                            possible_values=(
                                possible_values if possible_values else None
                            ),
                        )
                    )

                case _:
                    # multiple_choice
                    assert len(group.varnames) == len(group.labels)
                    question_type = "multiple_choice"
                    varnames = group.varnames
                    labels = group.labels
                    for idx, varname in enumerate(varnames):
                        label_text = ""
                        if idx < len(labels):
                            label = labels[idx]
                            if label:
                                label_text = label.get("de", "")
                        variables.append(
                            make_var(varname, pl.Boolean, label=label_text)
                        )

        case ("CHOICE", 2):
            # multiple_choice_other
            other_group = groups[1]
            assert len(other_group.items) == 1
            assert len(other_group.varnames) == 1
            assert ".1" in other_group.varnames[0]

            assert other_group.inputType.value == "SINGLELINE"
            primary_group = groups[0]
            assert len(primary_group.varnames) > 1
            assert len(primary_group.varnames) == len(primary_group.labels)
            question_type = "multiple_choice_other"

            varnames = primary_group.varnames
            other_text_var = other_group.varnames[0]
            other_var_prefix = other_text_var.split(".")[0]
            assert (
                other_var_prefix in varnames
            ), f"Other text variable prefix '{other_var_prefix}' not found in primary group varnames: {varnames}"

            other_boolean_idx = varnames.index(other_var_prefix)
            labels = primary_group.labels

            for idx, varname in enumerate(varnames):
                if idx == other_boolean_idx:
                    continue

                label_text = ""
                if idx < len(labels):
                    label = labels[idx]
                    if label:
                        label_text = label.get("de", "")

                variables.append(
                    make_var(
                        varname, pl.Boolean, label=label_text, is_other_boolean=False
                    )
                )

            other_boolean_label = ""
            if other_boolean_idx < len(labels):
                label = labels[other_boolean_idx]
                if label:
                    other_boolean_label = label.get("de", "")

            variables.append(
                make_var(
                    varnames[other_boolean_idx],
                    pl.Boolean,
                    label=other_boolean_label,
                    is_other_boolean=True,
                )
            )

            other_text_label = f"{other_boolean_label} (Text)"
            variables.append(
                make_var(
                    other_group.varnames[0],
                    pl.String,
                    group_id=1,
                    label=other_text_label,
                    is_other_text=True,
                )
            )

        case ("SCALE", 1):
            # scale
            group = groups[0]
            assert group.range is not None
            question_type = "scale"
            range_min = group.range[0] if len(group.range) > 0 else None
            range_max = group.range[1] if len(group.range) > 1 else None

            variables.append(
                make_var(
                    group.varnames[0],
                    pl.Int64,
                    range_min=range_min,
                    range_max=range_max,
                )
            )

        case _:
            raise RuntimeError(
                f"Unknown Question Type: {lp_question_type} with {number_groups} groups"
            )

    # Build question metadata dict
    question_dict = {
        "text": question.question.get("de", ""),
        "id": question.id,
        "absolute_position": absolute_position,
        "relative_section_position": question.position,
        "type": question.type.value,
        "number_groups": len(question.groups),
        "page_number": page_number,
        "question_type": question_type,
    }

    return question_dict, variables
