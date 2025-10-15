import polars as pl


def rename_results_columns(
    results_df: pl.DataFrame, metadata_df: pl.DataFrame
) -> pl.DataFrame:
    """Rename V* columns in results to Q* names using variable metadata."""
    # Build rename mapping from variable_df
    rename_mapping = dict(
        zip(metadata_df["original_id"].to_list(), metadata_df["id"].to_list())
    )

    # Add metadata column mappings
    lp_metadata_rename_mapping = {
        "vID": "id",
        "vCOMPLETED": "completed",
        "vFINISHED": "finished",
        "vDURATION": "duration",
        "vQUOTE": "quote",
        "vSTART": "start",
        "vEND": "end",
        "vRUNTIME": "runtime",
        "vPAGETIME1": "pagetime1",
        "vPAGETIME2": "pagetime2",
        "vPAGETIME3": "pagetime3",
        "vDATE": "date",
    }
    rename_mapping = {**lp_metadata_rename_mapping, **rename_mapping}

    # Only rename columns that exist in results_df
    results_columns = set(results_df.columns)
    filtered_mapping = {
        old: new for old, new in rename_mapping.items() if old in results_columns
    }

    return results_df.rename(filtered_mapping)
