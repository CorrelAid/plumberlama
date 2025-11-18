import pandera.polars as pa
import polars as pl

from plumberlama.type_mapping import string_to_polars

question_types = [
    "matrix",
    "multiple_choice",
    "single_choice",
    "multiple_choice_other",
    "input_single_singleline",
    "input_single_integer",
    "input_single_multiline",
    "input_multiple_singleline",
    "input_multiple_integer",
    "input_multiple_multiline",
    "scale",
]

anonymization_types = ["shuffle", "aggregate", "yeet"]


class DistributionsSchema(pa.DataFrameModel):
    """Schema for anonymized distributions table.

    Contains shuffled individual values for numeric questions (scale, matrix,
    numeric inputs) to preserve statistical distributions while breaking
    respondent linkage.
    """

    class Config:
        coerce = True  # Allow type coercion (e.g., Int64 -> Float64)

    variable_id: str
    value: float  # Numeric values (can be int or float)
    load_counter: int = pa.Field(ge=0)  # Wave number, starts at 0


class CategoricalSchema(pa.DataFrameModel):
    """Schema for anonymized categorical table.

    Contains aggregated counts for categorical questions (single choice,
    multiple choice) with no individual response data retained.
    """

    class Config:
        coerce = True  # Allow type coercion

    variable_id: str
    value: str  # Categorical value (choice label or boolean string)
    count: int = pa.Field(ge=0)  # Aggregated count for this value
    load_counter: int = pa.Field(ge=0)  # Wave number, starts at 0


class ParsedMetadataSchema(pa.DataFrameModel):
    """Schema for parsed metadata DataFrame at variable level with question text joined.

    This is the schema after parsing metadata from API, with one row per variable
    and question text already joined in.
    """

    question_id: int
    group_id: int
    id: str
    question_position: int
    question_type: pl.Enum(question_types)
    schema_variable_type: (
        str  # string representation of Polars DataType (e.g., "Int64", "String")
    )
    anonymization_type: pl.Enum(anonymization_types)
    question_text: str
    label: str = pa.Field(nullable=True)
    range_min: int = pa.Field(nullable=True)
    range_max: int = pa.Field(nullable=True)
    possible_values_codes: pl.List(pl.String) = pa.Field(nullable=True)
    possible_values_labels: pl.List(pl.String) = pa.Field(nullable=True)
    scale_labels: pl.List(pl.String) = pa.Field(nullable=True)
    is_other_boolean: bool
    is_other_text: bool
    validation_warning: str = pa.Field(nullable=True, coerce=True)


class ProcessedMetadataSchema(ParsedMetadataSchema):
    """Schema for processed metadata DataFrame with renamed variables.

    Extends ParsedMetadataSchema by adding original_id field to track the mapping
    from original variable names (e.g., V1, V2) to renamed variables (e.g., Q1, Q2_age),
    and anonymized_table to indicate which database table contains the anonymized data.
    """

    original_id: str
    anonymized_table: str = pa.Field(
        nullable=True
    )  # "_distributions", "_categorical", or null for "none"/"yeet"


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
