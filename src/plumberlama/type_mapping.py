from typing import Dict, Tuple

import polars as pl
from sqlalchemy import Boolean, DateTime, Float, Integer, Text
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.types import TypeEngine

# Core type registry: Polars type → (string representation, SQLAlchemy type)
_TYPE_REGISTRY: Dict[pl.DataType, Tuple[str, TypeEngine]] = {
    pl.Int64: ("Int64", Integer),
    pl.Int32: ("Int32", Integer),
    pl.Float64: ("Float64", Float),
    pl.Float32: ("Float32", Float),
    pl.String: ("String", Text),
    pl.Utf8: ("String", Text),  # Alias for String
    pl.Boolean: ("Boolean", Boolean),
    pl.Date: ("Date", DateTime),
    pl.Datetime: ("Datetime", DateTime),
    pl.Object: ("Object", Text),  # Objects stored as text in DB
}


def polars_to_string(polars_type: pl.DataType) -> str:
    """Convert Polars type to string representation."""
    # Handle List types recursively
    if isinstance(polars_type, pl.datatypes.List):
        inner_str = polars_to_string(polars_type.inner)
        return f"List({inner_str})"

    # Handle Enum types - serialize as String representation
    if isinstance(polars_type, pl.datatypes.Enum):
        categories = polars_type.categories
        return f"Enum({categories!r})"

    # Handle scalar types via registry lookup
    if polars_type in _TYPE_REGISTRY:
        return _TYPE_REGISTRY[polars_type][0]

    # Fallback: clean up string representation
    type_str = str(polars_type)
    return type_str.replace("DataType", "").strip()


def string_to_polars(type_str: str) -> pl.DataType:
    """Convert string representation to Polars type."""
    # Handle List types recursively
    if type_str.startswith("List(") and type_str.endswith(")"):
        inner_str = type_str[5:-1]  # Extract content between "List(" and ")"
        inner_type = string_to_polars(inner_str)
        return pl.List(inner_type)

    # Handle Enum types - deserialize to String (we don't reconstruct Enum categories)
    if type_str.startswith("Enum("):
        return pl.String

    # Reverse lookup in registry
    for pl_type, (str_repr, _) in _TYPE_REGISTRY.items():
        if str_repr == type_str:
            return pl_type

    # Fallback: try eval (safe since we control the input from our own serialization)
    try:
        return eval(f"pl.{type_str}")
    except (AttributeError, SyntaxError):
        raise ValueError(f"Cannot parse DataType string: {type_str}")


def polars_to_sqlalchemy(polars_type: pl.DataType) -> TypeEngine:
    """Convert Polars type to SQLAlchemy type."""
    # Handle List types → PostgreSQL ARRAY
    if isinstance(polars_type, pl.datatypes.List):
        inner_sqlalchemy = polars_to_sqlalchemy(polars_type.inner)
        return ARRAY(inner_sqlalchemy)

    # Handle Enum types → TEXT
    if isinstance(polars_type, pl.datatypes.Enum):
        return Text

    # Handle scalar types via registry lookup
    if polars_type in _TYPE_REGISTRY:
        return _TYPE_REGISTRY[polars_type][1]

    # Fallback to Text for unknown types
    return Text


# Backward compatibility alias
parse_datatype = string_to_polars
