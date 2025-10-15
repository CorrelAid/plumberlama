import pandera.polars as pa
import polars as pl

from plumberlama.type_mapping import string_to_polars


class ParsedMetadataSchema(pa.DataFrameModel):
    """Schema for parsed metadata DataFrame at variable level with question text joined.

    This is the schema after parsing metadata from API, with one row per variable
    and question text already joined in.
    """

    question_id: int
    group_id: int
    id: str
    question_position: int
    question_type: str
    schema_variable_type: (
        str  # string representation of Polars DataType (e.g., "Int64", "String")
    )
    question_text: str
    label: str = pa.Field(nullable=True)
    range_min: float = pa.Field(nullable=True)
    range_max: float = pa.Field(nullable=True)
    possible_values_codes: pl.List(pl.String) = pa.Field(nullable=True)
    possible_values_labels: pl.List(pl.String) = pa.Field(nullable=True)
    scale_labels: pl.List(pl.String) = pa.Field(nullable=True)
    is_other_boolean: bool
    is_other_text: bool


class ProcessedMetadataSchema(ParsedMetadataSchema):
    """Schema for processed metadata DataFrame with renamed variables.

    Extends ParsedMetadataSchema by adding original_id field to track the mapping
    from original variable names (e.g., V1, V2) to renamed variables (e.g., Q1, Q2_age).
    """

    original_id: str


def make_results_schema(variable_df: pl.DataFrame) -> pa.DataFrameSchema:
    """Create a Pandera schema for validating survey results data."""
    columns = {}

    for var in variable_df.to_dicts():
        var_id = var["id"]
        var_type = string_to_polars(var["schema_variable_type"])
        checks = []

        if var["question_type"] == "single_choice" and var.get(
            "possible_values_labels"
        ):
            label_values = var["possible_values_labels"]
            if label_values:
                var_type = pl.Enum(list(dict.fromkeys(label_values)))
                checks.append(pa.Check.isin(label_values))

        if var["question_type"] in ["scale", "matrix"]:
            range_min = var.get("range_min")
            range_max = var.get("range_max")
            if range_min is not None or range_max is not None:
                checks.append(pa.Check.in_range(range_min, range_max))

        columns[var_id] = pa.Column(var_type, checks=checks, nullable=True)

    metadata_columns = {
        "id": pa.Column(pl.Int64, nullable=False),
        "completed": pa.Column(pl.Boolean, nullable=False),
        "finished": pa.Column(pl.Boolean, nullable=False),
        "duration": pa.Column(pl.Float64, nullable=False),
        "quote": pa.Column(pl.String, nullable=False),
        "start": pa.Column(pl.Datetime("us"), nullable=False),
        "end": pa.Column(pl.Datetime("us"), nullable=False),
        "runtime": pa.Column(pl.String, nullable=False),
        "pagetime1": pa.Column(pl.Int64, nullable=False),
        "pagetime2": pa.Column(pl.Int64, nullable=False),
        "pagetime3": pa.Column(pl.Int64, nullable=False),
        "date": pa.Column(pl.Date, nullable=False),
    }

    return pa.DataFrameSchema({**columns, **metadata_columns}, strict=False)
