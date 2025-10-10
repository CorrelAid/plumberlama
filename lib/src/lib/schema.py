"""Schema definition and validation for survey data."""

import polars as pl
from typing import Dict, Any, Optional


class SurveySchema:
    """Polars Schema with validation rules for survey data."""

    def __init__(self, variable_df: pl.DataFrame):
        schema_dict = {}
        self.validation_rules = {}

        # Build from variables
        for var in variable_df.to_dicts():
            var_id = var["id"]
            var_type = var["type"]

            # For single_choice, use Enum type with decoded label values
            if var["question_type"] == "single_choice" and var.get("possible_values"):
                # Filter out empty/null values and get unique labels
                label_values = []
                seen = set()
                for v in var["possible_values"].values():
                    if v and v.strip() and v not in seen:
                        label_values.append(v)
                        seen.add(v)

                if label_values:
                    var_type = pl.Enum(label_values)
                    self.validation_rules[var_id] = {
                        "type": "enum",
                        "values": label_values,
                        "possible_values": var["possible_values"]
                    }

            # Add range constraints for scale and matrix
            if var["question_type"] in ["scale", "matrix"]:
                range_rule = {"type": "range"}
                if var.get("range_min") is not None:
                    range_rule["min"] = var["range_min"]
                if var.get("range_max") is not None:
                    range_rule["max"] = var["range_max"]
                if len(range_rule) > 1:  # Has min or max
                    self.validation_rules[var_id] = range_rule

            schema_dict[var_id] = var_type

        # Create Polars Schema with variables and metadata
        self.schema = pl.Schema({
            **schema_dict,
            # Metadata variables (common to all polls)
            "vID": pl.Int64,
            "vANONYM": pl.Int64,
            "vCOMPLETED": pl.Boolean,
            "vFINISHED": pl.Boolean,
            "vDURATION": pl.Float64,
            "vQUOTE": pl.String,
            "vLANG": pl.String,
            "vSTART": pl.Datetime("us"),
            "vEND": pl.Datetime("us"),
            "vRUNTIME": pl.String,
            "vPAGETIME1": pl.Int64,
            "vPAGETIME2": pl.Int64,
            "vPAGETIME3": pl.Int64,
            "vDATE": pl.Date,
        })

    def get_type(self, var_id: str) -> Optional[pl.DataType]:
        """Get the Polars data type for a variable."""
        return self.schema.get(var_id)

    def get_enum_values(self, var_id: str) -> Optional[list]:
        """Get the enum values for a variable if it's an Enum type."""
        rule = self.validation_rules.get(var_id, {})
        if rule.get("type") == "enum":
            return rule.get("values")
        return None

    def get_range(self, var_id: str) -> tuple:
        """Get (min, max) range for a variable."""
        rule = self.validation_rules.get(var_id, {})
        if rule.get("type") == "range":
            return (rule.get("min"), rule.get("max"))
        return (None, None)

    def to_dict(self) -> Dict[str, Dict[str, Any]]:
        """Convert schema to dict."""
        result = {}
        for var_id in self.schema.names():
            result[var_id] = {
                "type": self.schema[var_id],
            }
            # Add validation rules if present
            if var_id in self.validation_rules:
                result[var_id].update(self.validation_rules[var_id])
        return result

    def validate_and_cast(self, results_df: pl.DataFrame, validate: bool = True) -> pl.DataFrame:
        """Cast results to schema types and validate constraints."""
        for col in self.schema.names():
            if col not in results_df.columns:
                continue

            dtype = self.schema[col]

            # Cast to appropriate type
            if dtype == pl.Boolean:
                results_df = results_df.with_columns(
                    pl.col(col).cast(pl.String, strict=False).is_in(["1", "true", "True"]).alias(col)
                )
            elif dtype == pl.Datetime:
                results_df = results_df.with_columns(
                    pl.col(col).str.to_datetime(strict=False)
                )
            elif dtype == pl.Date:
                results_df = results_df.with_columns(
                    pl.col(col).str.to_date(strict=False)
                )
            elif isinstance(dtype, pl.Enum):
                # Enum types are cast after decoding in decode_single_choice
                # Here we just ensure it's a string first
                results_df = results_df.with_columns(
                    pl.col(col).cast(pl.String, strict=False).alias(col)
                )
            else:
                results_df = results_df.with_columns(
                    pl.col(col).cast(dtype, strict=False)
                )

            # Apply validation if requested
            if validate and col in self.validation_rules:
                rule = self.validation_rules[col]
                # Range validation for scale
                if rule.get("type") == "range":
                    expr = pl.col(col)
                    if "min" in rule:
                        expr = pl.when((expr >= rule["min"]) | expr.is_null()).then(expr).otherwise(None)
                    if "max" in rule:
                        expr = pl.when((expr <= rule["max"]) | expr.is_null()).then(expr).otherwise(None)
                    results_df = results_df.with_columns(expr.alias(col))

        return results_df

    def decode_single_choice(self, results_df: pl.DataFrame) -> pl.DataFrame:
        """Decode single choice codes to labels and cast to Enum."""
        for var_id in self.schema.names():
            if var_id not in results_df.columns:
                continue

            # Check if this variable has enum validation rules
            rule = self.validation_rules.get(var_id, {})
            if rule.get("type") == "enum":
                possible_values = rule.get("possible_values")

                if possible_values:
                    # Build code to label mapping (filter out empty codes/labels)
                    code_to_label = {
                        int(code): label
                        for code, label in possible_values.items()
                        if code and code.strip() and label and label.strip()
                    }

                    if code_to_label:
                        # Get unique enum values for casting
                        enum_values = []
                        seen = set()
                        for label in code_to_label.values():
                            if label not in seen:
                                enum_values.append(label)
                                seen.add(label)

                        # Decode and cast to Enum
                        results_df = results_df.with_columns(
                            pl.col(var_id)
                            .replace(code_to_label, default=None)
                            .cast(pl.Enum(enum_values))
                            .alias(var_id)
                        )

        return results_df


def build_schema_from_variables(variable_df: pl.DataFrame) -> Dict[str, Dict[str, Any]]:
    """
    Build schema dict from variable metadata (backward compatible function).

    Args:
        variable_df: DataFrame with variable metadata

    Returns:
        Dict mapping variable IDs to schema definitions
    """
    schema_obj = SurveySchema(variable_df)
    return schema_obj.to_dict()


def cast_results_to_schema(results_df: pl.DataFrame, schema: Dict[str, Dict[str, Any]], validate: bool = True) -> pl.DataFrame:
    """
    Cast results to schema types (backward compatible function).

    Args:
        results_df: DataFrame with results
        schema: Schema dict from build_schema_from_variables
        validate: If True, apply validation

    Returns:
        DataFrame with proper types
    """
    # Reconstruct schema_dict and validation_rules from dict format
    schema_dict = {}
    validation_rules = {}

    for var_id, var_info in schema.items():
        schema_dict[var_id] = var_info["type"]

        # Extract validation rules
        if "possible_values" in var_info:
            validation_rules[var_id] = {
                "type": "enum",
                "values": list(var_info["possible_values"].values()),
                "possible_values": var_info["possible_values"]
            }
        elif "min" in var_info or "max" in var_info:
            range_rule = {"type": "range"}
            if "min" in var_info:
                range_rule["min"] = var_info["min"]
            if "max" in var_info:
                range_rule["max"] = var_info["max"]
            validation_rules[var_id] = range_rule

    # Create temporary SurveySchema object
    temp_schema = SurveySchema(pl.DataFrame())
    temp_schema.schema = pl.Schema(schema_dict)
    temp_schema.validation_rules = validation_rules

    return temp_schema.validate_and_cast(results_df, validate)
