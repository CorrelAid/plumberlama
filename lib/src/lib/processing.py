import polars as pl


def decode_single_choice_vars(results_df, variable_df):
    """
    Decode single choice variables from codes to labels and cast to Enum type.
    """
    single_choice_vars = variable_df.filter(pl.col("question_type") == "single_choice")

    for var_row in single_choice_vars.to_dicts():
        var_id = var_row["id"]
        possible_values = var_row.get("possible_values")

        if possible_values and var_id in results_df.columns:
            # Build code to label mapping (codes are strings in possible_values keys)
            code_to_label = {int(code): label for code, label in possible_values.items() if code.strip()}

            if code_to_label:
                # Get all possible label values for the Enum
                enum_values = list(code_to_label.values())

                # Decode codes to labels and cast to Enum
                results_df = results_df.with_columns(
                    pl.col(var_id)
                    .replace(code_to_label, default=None)
                    .cast(pl.Enum(enum_values))
                    .alias(var_id)
                )

    return results_df