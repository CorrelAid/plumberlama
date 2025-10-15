import pandera.polars as pa
import polars as pl


def decode_single_choice(
    schema: pa.DataFrameSchema, results_df: pl.DataFrame
) -> pl.DataFrame:
    """Decode single choice variables using schema information."""
    for var_id, col_schema in schema.columns.items():
        if var_id not in results_df.columns:
            continue

        # Check if dtype is Enum by checking string representation
        dtype_str = str(col_schema.dtype)
        if "Enum" in dtype_str:
            # Extract enum categories from the isin check
            check_values = [
                check.statistics.get("allowed_values")
                for check in col_schema.checks
                if hasattr(check, "statistics") and "allowed_values" in check.statistics
            ]

            if check_values:
                label_list = list(check_values[0])
                # Map codes (1,2,3,...) to labels
                code_to_label = {
                    str(i + 1): label for i, label in enumerate(label_list)
                }

                # Decode string codes to labels, keeping empty strings as empty
                results_df = results_df.with_columns(
                    pl.when(
                        pl.col(var_id).cast(pl.String, strict=False).str.strip_chars()
                        == ""
                    )
                    .then(None)
                    .otherwise(
                        pl.col(var_id)
                        .cast(pl.String, strict=False)
                        .replace_strict(
                            code_to_label,
                            default=pl.col(var_id),
                            return_dtype=pl.String,
                        )
                    )
                    .alias(var_id)
                )

    return results_df
