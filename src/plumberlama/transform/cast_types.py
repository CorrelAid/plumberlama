import pandera.polars as pa
import polars as pl


def cast_results_to_schema(
    results_df: pl.DataFrame, schema: pa.DataFrameSchema
) -> pl.DataFrame:
    """Cast columns to types expected by schema."""
    cast_exprs = []

    for col_name, col_schema in schema.columns.items():
        if col_name not in results_df.columns:
            continue

        # Get the underlying polars type
        dtype_str = str(col_schema.dtype)

        # Cast based on expected type
        if "Boolean" in dtype_str:
            # Convert int/string to boolean (1/"1" -> True, 0/"0" -> False, empty -> None)
            cast_exprs.append(
                pl.when(pl.col(col_name).cast(pl.String).is_in(["1"]))
                .then(True)
                .when(pl.col(col_name).cast(pl.String).is_in(["0", ""]))
                .then(False)
                .otherwise(None)
                .alias(col_name)
            )
        elif "Int64" in dtype_str or "Int32" in dtype_str:
            # Convert string to int, empty strings become null
            # First cast to string, strip whitespace, replace empty with null, then cast to int
            cast_exprs.append(
                pl.col(col_name)
                .cast(pl.String)
                .str.strip_chars()
                .replace("", None)
                .cast(pl.Int64, strict=True)
                .alias(col_name)
            )
        elif "Float64" in dtype_str or "Float32" in dtype_str:
            # Convert string to float, empty strings become null
            # First cast to string, strip whitespace, replace empty with null, then cast to float
            cast_exprs.append(
                pl.col(col_name)
                .cast(pl.String)
                .str.strip_chars()
                .replace("", None)
                .cast(pl.Float64, strict=True)
                .alias(col_name)
            )
        elif "Date" in dtype_str and "time" not in dtype_str:
            # Parse date strings
            cast_exprs.append(pl.col(col_name).str.to_date(strict=True).alias(col_name))
        elif "Datetime" in dtype_str:
            # Parse datetime strings
            cast_exprs.append(
                pl.col(col_name).str.to_datetime(strict=True).alias(col_name)
            )
        elif "Enum" in dtype_str:
            # Cast to Enum type - extract categories from dtype string
            import re

            match = re.search(r"Enum\(categories=\[(.*?)\]\)", dtype_str)
            if match:
                categories_str = match.group(1)
                # Parse the categories list
                categories = eval(
                    f"[{categories_str}]"
                )  # Safe since we control the schema
                cast_exprs.append(
                    pl.col(col_name)
                    .cast(pl.Enum(categories), strict=True)
                    .alias(col_name)
                )
        # For String types, keep as-is

    if cast_exprs:
        results_df = results_df.with_columns(cast_exprs)

    return results_df
