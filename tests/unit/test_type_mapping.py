"""Unit tests for type mapping module."""

import polars as pl
import pytest
from sqlalchemy import Boolean, DateTime, Float, Integer, Text
from sqlalchemy.dialects.postgresql import ARRAY

from plumberlama.type_mapping import (
    polars_to_sqlalchemy,
    polars_to_string,
    string_to_polars,
)


class TestPolarsToString:
    """Test Polars → String conversion."""

    def test_scalar_types(self):
        """Test conversion of scalar types."""
        assert polars_to_string(pl.Int64) == "Int64"
        assert polars_to_string(pl.Int32) == "Int32"
        assert polars_to_string(pl.Float64) == "Float64"
        assert polars_to_string(pl.Float32) == "Float32"
        assert polars_to_string(pl.String) == "String"
        assert polars_to_string(pl.Boolean) == "Boolean"
        assert polars_to_string(pl.Date) == "Date"
        assert polars_to_string(pl.Datetime) == "Datetime"

    def test_list_types(self):
        """Test conversion of List types."""
        assert polars_to_string(pl.List(pl.String)) == "List(String)"
        assert polars_to_string(pl.List(pl.Int64)) == "List(Int64)"
        assert polars_to_string(pl.List(pl.Float64)) == "List(Float64)"

    def test_nested_list_types(self):
        """Test conversion of nested List types."""
        assert polars_to_string(pl.List(pl.List(pl.String))) == "List(List(String))"


class TestStringToPolars:
    """Test String → Polars conversion."""

    def test_scalar_types(self):
        """Test conversion of scalar types."""
        assert string_to_polars("Int64") == pl.Int64
        assert string_to_polars("Int32") == pl.Int32
        assert string_to_polars("Float64") == pl.Float64
        assert string_to_polars("Float32") == pl.Float32
        assert string_to_polars("String") == pl.String
        assert string_to_polars("Boolean") == pl.Boolean
        assert string_to_polars("Date") == pl.Date
        assert string_to_polars("Datetime") == pl.Datetime

    def test_list_types(self):
        """Test conversion of List types."""
        assert string_to_polars("List(String)") == pl.List(pl.String)
        assert string_to_polars("List(Int64)") == pl.List(pl.Int64)
        assert string_to_polars("List(Float64)") == pl.List(pl.Float64)

    def test_enum_types_fallback_to_string(self):
        """Test that Enum types deserialize to String."""
        result = string_to_polars("Enum(['a', 'b', 'c'])")
        assert result == pl.String

    def test_invalid_type_raises_error(self):
        """Test that invalid type strings raise ValueError."""
        with pytest.raises(ValueError):
            string_to_polars("InvalidType")


class TestPolarsToSQLAlchemy:
    """Test Polars → SQLAlchemy conversion."""

    def test_scalar_types(self):
        """Test conversion of scalar types."""
        # polars_to_sqlalchemy returns type classes, not instances
        assert polars_to_sqlalchemy(pl.Int64) == Integer
        assert polars_to_sqlalchemy(pl.Int32) == Integer
        assert polars_to_sqlalchemy(pl.Float64) == Float
        assert polars_to_sqlalchemy(pl.Float32) == Float
        assert polars_to_sqlalchemy(pl.String) == Text
        assert polars_to_sqlalchemy(pl.Boolean) == Boolean
        assert polars_to_sqlalchemy(pl.Datetime) == DateTime

    def test_list_types_to_array(self):
        """Test conversion of List types to PostgreSQL ARRAY."""
        result = polars_to_sqlalchemy(pl.List(pl.String))
        assert isinstance(result, ARRAY)
        assert isinstance(result.item_type, type(Text()))

        result = polars_to_sqlalchemy(pl.List(pl.Int64))
        assert isinstance(result, ARRAY)
        assert isinstance(result.item_type, type(Integer()))

    def test_object_type_to_text(self):
        """Test that Object type converts to Text."""
        assert polars_to_sqlalchemy(pl.Object) == Text


class TestRoundTrip:
    """Test round-trip conversions."""

    def test_polars_string_polars_roundtrip(self):
        """Test Polars → String → Polars round-trip."""
        types = [
            pl.Int64,
            pl.Int32,
            pl.Float64,
            pl.String,
            pl.Boolean,
            pl.List(pl.String),
            pl.List(pl.Int64),
        ]

        for original_type in types:
            string_repr = polars_to_string(original_type)
            recovered_type = string_to_polars(string_repr)
            assert recovered_type == original_type, f"Failed for {original_type}"
