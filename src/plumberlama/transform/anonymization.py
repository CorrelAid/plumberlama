import random

import polars as pl

from plumberlama.logging_config import get_logger

logger = get_logger(__name__)
sys_random = random.SystemRandom()


def shuffle(column_series: pl.Series, var_id: str) -> pl.DataFrame:
    """Shuffle values and return DataFrame with variable_id and value columns.

    Args:
        column_series: Series containing values to shuffle
        var_id: Variable identifier

    Returns:
        DataFrame with columns: variable_id, value
    """
    # Get non-null values as list
    values = column_series.drop_nulls().to_list()

    # Shuffle the list (creates new list, doesn't mutate)
    shuffled_values = values.copy()
    sys_random.shuffle(shuffled_values)

    # Return DataFrame with variable_id and shuffled values
    return pl.DataFrame(
        {"variable_id": [var_id] * len(shuffled_values), "value": shuffled_values}
    )


def aggregate(column_series: pl.Series, var_id: str) -> pl.DataFrame:
    """Aggregate values and return DataFrame with counts for each value.

    Args:
        column_series: Series containing values to aggregate
        var_id: Variable identifier

    Returns:
        DataFrame with columns: variable_id, value (as String), count
    """
    # Count occurrences of each value (excluding nulls)
    value_counts = column_series.drop_nulls().value_counts(sort=True)

    # value_counts returns a DataFrame with columns: <series_name> and "count"
    # Rename the value column to "value", cast to String, and add variable_id
    return (
        value_counts.rename({column_series.name: "value"})
        .with_columns(
            [
                pl.col("value").cast(
                    pl.String
                ),  # Cast to String for compatibility across types
                pl.lit(var_id).alias("variable_id"),
            ]
        )
        .select(["variable_id", "value", "count"])
    )


def anonymize_results(
    results_df: pl.DataFrame, metadata_df: pl.DataFrame, load_counter: int
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Anonymize survey results by shuffling or aggregating based on anonymization_type.

    Args:
        results_df: Processed results DataFrame with all survey responses
        metadata_df: Metadata DataFrame with variable information including anonymization_type
        load_counter: Load counter to track data waves

    Returns:
        Tuple of (distributions_df, categorical_df):
        - distributions_df: Shuffled individual values (variable_id, value, load_counter)
        - categorical_df: Aggregated counts (variable_id, value, count, load_counter)
    """
    logger.info("Anonymizing results...")

    distributions_list = []
    categorical_list = []

    # Get metadata columns (non-survey question columns)
    metadata_cols = {
        "id",
        "completed",
        "finished",
        "duration",
        "quote",
        "start",
        "end",
        "runtime",
        "date",
        "load_counter",
    }
    metadata_cols.update(
        [col for col in results_df.columns if col.startswith("pagetime")]
    )

    # Iterate through metadata to process each variable
    for row in metadata_df.iter_rows(named=True):
        var_id = row["id"]
        anonymization_type = row["anonymization_type"]

        # Skip if variable not in results (shouldn't happen but safeguard)
        if var_id not in results_df.columns:
            logger.warning(
                f"Variable {var_id} in metadata but not in results, skipping"
            )
            continue

        # Skip metadata columns
        if var_id in metadata_cols:
            continue

        # Skip if anonymization_type is "none" or "yeet"
        if anonymization_type in ("none", "yeet"):
            logger.debug(
                f"Skipping {var_id} (anonymization_type: {anonymization_type})"
            )
            continue

        column_series = results_df[var_id]

        # Determine which anonymization function to use based on anonymization_type
        if anonymization_type == "shuffle":
            result_df = shuffle(column_series, var_id)
            distributions_list.append(result_df)
        elif anonymization_type == "aggregate":
            result_df = aggregate(column_series, var_id)
            categorical_list.append(result_df)
        else:
            logger.warning(
                f"Unknown anonymization_type '{anonymization_type}' for {var_id}, skipping"
            )
            continue

    # Combine all distributions and categorical results
    if distributions_list:
        distributions_df = pl.concat(distributions_list)
        # Add load_counter
        distributions_df = distributions_df.with_columns(
            pl.lit(load_counter).alias("load_counter")
        )
    else:
        # Create empty DataFrame with correct schema
        distributions_df = pl.DataFrame(
            {
                "variable_id": pl.Series([], dtype=pl.String),
                "value": pl.Series([], dtype=pl.Float64),
                "load_counter": pl.Series([], dtype=pl.Int64),
            }
        )

    if categorical_list:
        categorical_df = pl.concat(categorical_list)
        # Add load_counter
        categorical_df = categorical_df.with_columns(
            pl.lit(load_counter).alias("load_counter")
        )
    else:
        # Create empty DataFrame with correct schema
        categorical_df = pl.DataFrame(
            {
                "variable_id": pl.Series([], dtype=pl.String),
                "value": pl.Series([], dtype=pl.String),
                "count": pl.Series([], dtype=pl.Int64),
                "load_counter": pl.Series([], dtype=pl.Int64),
            }
        )

    logger.info(
        f"   ✓ Created {len(distributions_df)} distribution records and {len(categorical_df)} categorical records"
    )

    return distributions_df, categorical_df
