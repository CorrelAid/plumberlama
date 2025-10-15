import pandera.polars as pa
import polars as pl


def decode_single_choice(
    schema: pa.DataFrameSchema, results_df: pl.DataFrame, metadata_df: pl.DataFrame
) -> pl.DataFrame:
    """Decode single choice variables using schema and metadata information.

    Args:
        schema: DataFrameSchema for validation
        results_df: Results DataFrame to decode
        metadata_df: Metadata DataFrame containing possible_values_codes and possible_values_labels

    Returns:
        Decoded results DataFrame
    """
    for var_id, col_schema in schema.columns.items():
        if var_id not in results_df.columns:
            continue

        # Check if dtype is Enum by checking string representation
        dtype_str = str(col_schema.dtype)
        if "Enum" in dtype_str:
            # Get the code-to-label mapping from metadata
            var_metadata = metadata_df.filter(pl.col("id") == var_id)
            if var_metadata.height == 0:
                continue

            codes = var_metadata.select("possible_values_codes").item(0, 0)
            labels = var_metadata.select("possible_values_labels").item(0, 0)

            if codes is not None and labels is not None and len(codes) == len(labels):
                code_to_label = dict(zip(codes, labels))

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
