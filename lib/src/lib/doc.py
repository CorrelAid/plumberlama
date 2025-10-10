import polars as pl

def create_survey_documentation(variable_df, question_df, results_df, schema, output_path="docs"):
    """Generate documentation organized by questions with tables grouped by question type.

    Generates MkDocs-compatible markdown files in the docs/ directory.
    """
    import os
    os.makedirs(output_path, exist_ok=True)

    doc_data = []
    results_columns = set(results_df.columns)

    for var in variable_df.to_dicts():
        var_id = var["id"]
        question_id = var["question_id"]
        question_type = var["question_type"]
        var_type = var["type"]

        question_row = question_df.filter(pl.col("id") == question_id).to_dicts()[0]
        question_text = question_row["text"]

        # Check if variable exists in results_df
        if var_id in results_columns:
            display_var_id = var_id
        else:
            display_var_id = var_id

        type_display = str(var_type).replace("DataType", "")

        # Extract range info from schema
        range_info = ""
        range_tuple = schema.get_range(var_id)
        if range_tuple[0] is not None or range_tuple[1] is not None:
            range_info = f"[{range_tuple[0]}, {range_tuple[1]}]"

        # Extract possible values from schema validation_rules
        possible_values = ""
        if var_id in schema.validation_rules:
            rule = schema.validation_rules[var_id]
            if rule.get("type") == "enum" and rule.get("values"):
                # Only show the decoded labels, not the codes
                values = rule["values"]
                # Filter out None values
                values = [v for v in values if v is not None]
                # Use semicolon separator (pipe breaks markdown tables)
                possible_values = "; ".join(values)

        # Extract label (empty string for single-variable questions)
        label = var.get("label", "")
        # Don't show "null" in documentation
        if label is None:
            label = ""

        doc_data.append({
            "Variable": display_var_id,
            "Label": label,
            "Question": question_text,
            "Question ID": question_id,
            "Question Type": question_type,
            "Data Type": type_display,
            "Range": range_info,
            "Possible Values": possible_values,
            "Question Position": var["question_position"]
        })

    doc_df = pl.DataFrame(doc_data).sort("Question Position")

    # Generate markdown organized by questions
    markdown = "# Survey Documentation\n\n"
    markdown += "## Overview\n\n"
    markdown += f"Total questions: {question_df.height}\n\n"
    markdown += f"Total variables: {doc_df.height}\n\n"

    markdown += "## Questions\n\n"

    with pl.Config(
        tbl_formatting="MARKDOWN",
        tbl_hide_column_data_types=True,
        tbl_hide_dataframe_shape=True,
        tbl_rows=-1,
        tbl_width_chars=1000,
        fmt_str_lengths=1000,
    ):
        # Group by question and iterate in position order
        for question_id in doc_df["Question ID"].unique().sort():
            question_vars = doc_df.filter(pl.col("Question ID") == question_id)

            # Get question details
            first_var = question_vars.row(0, named=True)
            question_text = first_var["Question"]
            question_type = first_var["Question Type"]
            question_position = first_var["Question Position"]

            markdown += f"### Q{question_position}: {question_text}\n\n"
            markdown += f"**Type:** {question_type}\n\n"
            markdown += f"**Variables:** {question_vars.height}\n\n"

            # Select columns based on what's relevant for this question type
            if question_type == "scale":
                cols = ["Variable", "Data Type", "Range"]
            elif question_type == "matrix":
                cols = ["Variable", "Label", "Data Type", "Range"]
            elif question_type == "single_choice":
                cols = ["Variable", "Data Type", "Possible Values"]
            elif question_type == "input_single_singleline":
                cols = ["Variable", "Data Type"]
            elif question_type == "input_single_multiline":
                cols = ["Variable", "Data Type"]
            elif question_type.startswith("input_"):
                cols = ["Variable", "Label", "Data Type"]
            elif question_type.startswith("multiple_choice"):
                cols = ["Variable", "Label", "Data Type"]
            else:
                cols = ["Variable", "Label", "Data Type"]

            # Filter to only include columns that exist and have non-empty values
            available_cols = [c for c in cols if c in question_vars.columns]
            table_str = str(question_vars.select(available_cols))

            markdown += table_str + "\n\n"

    # Write survey documentation
    with open(os.path.join(output_path, "survey_documentation.md"), "w", encoding="utf-8") as f:
        f.write(markdown)

    # Create index page for MkDocs
    index_content = """# U25 Survey Documentation

Welcome to the U25 survey documentation. This documentation provides detailed information about the survey questions, variables, and data structure.

## Quick Navigation

- **[Survey Documentation](survey_documentation.md)** - Complete list of all questions and variables

## Overview

This documentation is automatically generated from the survey definition and includes:

- Question text and types
- Variable names and data types
- Valid ranges for numeric questions
- Possible values for choice questions
- Variable labels for multi-variable questions

## About

- **Total Questions:** {total_questions}
- **Total Variables:** {total_variables}
- **Survey ID:** U25

Use the search function (top right) to quickly find specific questions or variables.
""".format(total_questions=question_df.height, total_variables=doc_df.height)

    with open(os.path.join(output_path, "index.md"), "w", encoding="utf-8") as f:
        f.write(index_content)

    return doc_df