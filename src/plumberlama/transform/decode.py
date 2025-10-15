import pandera.polars as pa
import polars as pl


def decode_single_choice(
    schema: pa.DataFrameSchema, results_df: pl.DataFrame, metadata_df: pl.DataFrame
) -> pl.DataFrame:
    """Decode single choice variables using schema and metadata information."""
    for var_id, col_schema in schema.columns.items():
        # Check if dtype is Enum by checking string representation
        dtype_str = str(col_schema.dtype)
        if "Enum" in dtype_str:
            # Get the code-to-label mapping from metadata
            var_metadata = metadata_df.filter(pl.col("id") == var_id)

            codes = var_metadata.select("possible_values_codes").item(0, 0)
            labels = var_metadata.select("possible_values_labels").item(0, 0)

            code_to_label = dict(zip(codes, labels))

            # Decode string codes to labels
            results_df = results_df.with_columns(
                pl.col(var_id)
                .cast(pl.String, strict=False)
                .replace_strict(
                    code_to_label,
                    default=pl.col(var_id),
                    return_dtype=pl.String,
                )
                .alias(var_id)
            )

    return results_df
