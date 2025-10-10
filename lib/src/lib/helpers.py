import polars as pl


def make_question_df(questions):
    unique_pages = list(set([question["pageId"] for question in questions]))
    pages_dict = dict(zip(unique_pages, range(1, len(unique_pages) + 1)))

    return pl.DataFrame(
        {
            "text": [q["question"]["de"] for q in questions],
            "id": [q["id"] for q in questions],
            "absolute_position": list(range(1, len(questions) + 1)),
            "relative_section_position": [q["position"] for q in questions],
            "type": [question["type"] for question in questions],
            "number_groups": [len(question["groups"]) for question in questions],
            "page_number": [pages_dict[question["pageId"]] for question in questions],
        }
    )


def make_group_df(questions):
    groups_data = []
    for q in questions:
        question_id = q["id"]
        for i, group in enumerate(q["groups"]):
            group_id = i
            group_name = group.get("name", {})
            groups_data.append(
                {
                    "question_id": question_id,
                    "id": group_id,
                    "group_name_de": group_name.get("de")
                    if isinstance(group_name, dict)
                    else None,
                    "number_varnames": len(group.get("varnames", [])),
                    "number_items": len(group.get("items", [])),
                    "number_labels": len(group.get("labels", [])),
                    "range": not group["range"] is not None,
                    "varnames": group["varnames"],
                }
            )

    return pl.DataFrame(groups_data)


def make_item_df(questions):
    items_data = []
    for q in questions:
        question_id = q["id"]
        for i, group in enumerate(q["groups"]):
            group_id = i
            input_type = group.get("inputType", None)
            for item in group.get("items", []):
                item_name = item.get("name", {})
                items_data.append(
                    {
                        "question_id": question_id,
                        "id": item.get("id"),
                        "item_name_de": item_name.get("de")
                        if isinstance(item_name, dict)
                        else None,
                        "input_type": input_type,
                        "group_id": group_id,
                    }
                )

    if not items_data:
        return pl.DataFrame(schema={
            "question_id": pl.Int64,
            "id": pl.Int64,
            "item_name_de": pl.String,
            "input_type": pl.String,
            "group_id": pl.Int64
        })
    return pl.DataFrame(items_data)


def get_question_type_and_variables(qrow, df_groups, df_items, question_obj):
    """Determine question type and generate variable metadata."""
    question_id = qrow["id"]
    absolute_position = qrow["absolute_position"]
    lp_question_type = qrow["type"]
    number_groups = qrow["number_groups"]
    groups = df_groups.filter(pl.col("question_id") == question_id).to_dicts()
    items = df_items.filter(pl.col("question_id") == question_id)

    variables = []

    def make_var(var_id, var_type, group_id=0, **extras):
        """Helper to create variable dict with common fields."""
        return {
            "question_id": question_id,
            "group_id": group_id,
            "id": var_id,
            "question_position": absolute_position,
            "question_type": question_type,
            "type": var_type,
            **extras
        }

    ##################### input_single_<input_type> #####################
    if lp_question_type == "INPUT":
        if number_groups == 1:
            group = groups[0]
            assert group["number_labels"] == 0
            assert group["number_varnames"] == 1
            assert group["number_items"] == 1
            item = items.to_dicts()[0]
            assert item["input_type"] in ["SINGLELINE", "INTEGER", "MULTILINE"]
            question_type = f"input_single_{item['input_type'].lower()}"
            var_type = pl.Int64 if item["input_type"] == "INTEGER" else pl.String
            variables.append(make_var(group["varnames"][0], var_type))
            return question_type, variables

        ##################### input_multiple_<input_type> #####################
        elif number_groups > 1:
            input_types = []
            for group in groups:
                assert group["number_varnames"] == 1
                assert group["number_labels"] == 0
                assert group["number_items"] == 1
                assert group["group_name_de"]
                item = items.filter(pl.col("group_id") == group["id"]).to_dicts()[0]
                input_types.append(item["input_type"])
            assert len(list(set(input_types))) == 1
            question_type = f"input_multiple_{input_types[0].lower()}"
            var_type = pl.Int64 if input_types[0] == "INTEGER" else pl.String
            for idx, group in enumerate(question_obj["groups"]):
                varname = group["varnames"][0]
                label_text = ""
                group_name = group.get("name", {})
                if isinstance(group_name, dict):
                    label_text = group_name.get("de", "")

                if not label_text:
                    group_items = group.get("items", [])
                    if group_items:
                        item_name = group_items[0].get("name", "")
                        label_text = (
                            item_name.get("de")
                            if isinstance(item_name, dict)
                            else str(item_name)
                            if item_name
                            else ""
                        )

                variables.append(make_var(varname, var_type, group_id=idx, label=label_text))

            return question_type, variables

    ##################### matrix #####################
    elif lp_question_type == "MATRIX":
        assert number_groups == 1
        group = groups[0]
        assert group["number_items"] > 1
        assert group["number_varnames"] == group["number_items"]
        assert group["number_labels"] > 1
        question_type = "matrix"

        group_obj = question_obj["groups"][0]
        varnames = group_obj.get("varnames", [])
        group_items = group_obj.get("items", [])
        labels = group_obj.get("labels", [])

        # Extract range information for matrix questions
        # Try range first
        range_obj = group_obj.get("range")
        if range_obj and isinstance(range_obj, list) and len(range_obj) >= 2:
            range_min = range_obj[0]
            range_max = range_obj[1]
        # If no range, infer from labels (common for Likert scales)
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
                if isinstance(item, dict):
                    item_name = item.get("name", "")
                    item_text = (
                        item_name.get("de")
                        if isinstance(item_name, dict)
                        else str(item_name)
                        if item_name
                        else ""
                    )

            variables.append(make_var(
                varname,
                pl.Int64,
                label=item_text,
                range_min=range_min,
                range_max=range_max
            ))

        return question_type, variables

    elif lp_question_type == "CHOICE":
        ##################### single_choice #####################
        if number_groups == 1:
            group = groups[0]
            if group["number_varnames"] == 1:
                assert group["number_items"] == 1
                question_type = "single_choice"
                group_obj = question_obj["groups"][0]
                codes = group_obj.get("codes", [])
                labels = group_obj.get("labels", [])
                possible_values = {}
                if labels:
                    # If codes are empty/missing, generate them as 1, 2, 3, ...
                    if not codes or all(not c or not c.strip() for c in codes):
                        codes = [str(i+1) for i in range(len(labels))]

                    if len(codes) == len(labels):
                        for code, label in zip(codes, labels):
                            if code and code.strip():
                                label_text = label.get("de", "") if isinstance(label, dict) else str(label)
                                if label_text:
                                    possible_values[code] = label_text

                variables.append(make_var(
                    group["varnames"][0],
                    pl.String,
                    possible_values=possible_values if possible_values else None
                ))
                return question_type, variables

            ##################### multiple_choice #####################
            else:
                assert group["number_varnames"] == group["number_labels"]
                question_type = "multiple_choice"
                group_obj = question_obj["groups"][0]
                varnames = group_obj.get("varnames", [])
                labels = group_obj.get("labels", [])
                for idx, varname in enumerate(varnames):
                    label_text = ""
                    if idx < len(labels):
                        label = labels[idx]
                        if isinstance(label, dict):
                            label_text = label.get("de", "")
                        elif isinstance(label, list) and len(label) == 0:
                            label_text = ""
                        elif label:
                            label_text = str(label)
                    variables.append(make_var(varname, pl.Boolean, label=label_text))

                return question_type, variables

        ##################### multiple_choice_other #####################
        else:
            assert number_groups == 2
            other_group = groups[1]
            assert other_group["number_items"] == 1
            assert other_group["number_varnames"] == 1
            assert ".1" in other_group["varnames"][0]

            item = items.filter(pl.col("group_id") == other_group["id"]).to_dicts()[0]
            assert item["input_type"] == "SINGLELINE"
            primary_group = groups[0]
            assert primary_group["number_varnames"] > 1
            assert primary_group["number_varnames"] == primary_group["number_labels"]
            question_type = "multiple_choice_other"

            primary_group_obj = question_obj["groups"][0]
            varnames = primary_group_obj.get("varnames", [])
            other_text_var = other_group["varnames"][0]
            other_var_prefix = other_text_var.split(".")[0]
            assert other_var_prefix in varnames, (
                f"Other text variable prefix '{other_var_prefix}' not found in primary group varnames: {varnames}"
            )

            other_boolean_idx = varnames.index(other_var_prefix)
            labels = primary_group_obj.get("labels", [])

            for idx, varname in enumerate(varnames):
                if idx == other_boolean_idx:
                    continue

                label_text = ""
                if idx < len(labels):
                    label = labels[idx]
                    assert isinstance(label, dict)
                    label_text = label.get("de")

                variables.append(make_var(varname, pl.Boolean, label=label_text, is_other_boolean=False))

            other_group_obj = question_obj["groups"][1]
            other_boolean_label = ""
            if other_boolean_idx < len(labels):
                label = labels[other_boolean_idx]
                if isinstance(label, dict):
                    other_boolean_label = label.get("de", "")
                elif isinstance(label, list) and len(label) == 0:
                    other_boolean_label = ""
                else:
                    raise ValueError("Unknown label type")

            # Default to "Other" if label is empty
            if not other_boolean_label:
                other_boolean_label = "Other"

            variables.append(make_var(
                varnames[other_boolean_idx],
                pl.Boolean,
                label=other_boolean_label,
                is_other_boolean=True
            ))

            other_text_label = f"{other_boolean_label} (Text)"
            variables.append(make_var(
                other_group_obj["varnames"][0],
                pl.String,
                group_id=1,
                label=other_text_label,
                is_other_text=True
            ))

            return question_type, variables

    ##################### scale #####################
    elif lp_question_type == "SCALE":
        assert number_groups == 1
        group = groups[0]
        assert group["range"] is not None
        question_type = "scale"
        range_obj = question_obj["groups"][0].get("range", [])
        range_min = range_obj[0] if len(range_obj) > 0 else None
        range_max = range_obj[1] if len(range_obj) > 1 else None

        variables.append(make_var(
            group["varnames"][0],
            pl.Int64,
            range_min=range_min,
            range_max=range_max
        ))
        return question_type, variables
    ##################### raise #####################
    else:
        raise RuntimeError("Unknown Question Type")


def process_questions_and_variables(questions):
    """Process questions and generate variable metadata."""
    question_df = make_question_df(questions)
    group_df = make_group_df(questions)
    item_df = make_item_df(questions)

    # Create a question lookup for accessing original objects
    question_lookup = {q["id"]: q for q in questions}

    # Process each question row and collect variables
    question_types = []
    all_variables = []

    for qrow_dict in question_df.to_dicts():
        question_obj = question_lookup[qrow_dict["id"]]
        question_type, vars_for_question = get_question_type_and_variables(
            qrow_dict, group_df, item_df, question_obj
        )
        question_types.append(question_type)
        all_variables.extend(vars_for_question)

    # Add question types to dataframe
    question_df = question_df.with_columns(question_type=pl.Series(question_types))

    # Create variable dataframe
    variable_df = pl.DataFrame(all_variables)

    return question_df, variable_df
