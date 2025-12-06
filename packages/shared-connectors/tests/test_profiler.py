"""Tests for growthnav.connectors.discovery.profiler."""

from __future__ import annotations

from datetime import datetime

import pytest
from growthnav.connectors.discovery.profiler import ColumnProfile, ColumnProfiler


class TestColumnProfile:
    """Tests for ColumnProfile dataclass."""

    def test_null_percentage_with_values(self) -> None:
        """Test null_percentage property calculation with values."""
        profile = ColumnProfile(
            name="test_col",
            inferred_type="string",
            total_count=100,
            null_count=25,
            unique_count=75,
        )
        assert profile.null_percentage == 25.0

    def test_null_percentage_zero_nulls(self) -> None:
        """Test null_percentage property with no nulls."""
        profile = ColumnProfile(
            name="test_col",
            inferred_type="string",
            total_count=100,
            null_count=0,
            unique_count=100,
        )
        assert profile.null_percentage == 0.0

    def test_null_percentage_all_nulls(self) -> None:
        """Test null_percentage property with all nulls."""
        profile = ColumnProfile(
            name="test_col",
            inferred_type="string",
            total_count=100,
            null_count=100,
            unique_count=0,
        )
        assert profile.null_percentage == 100.0

    def test_null_percentage_empty_column(self) -> None:
        """Test null_percentage property with empty column."""
        profile = ColumnProfile(
            name="test_col",
            inferred_type="string",
            total_count=0,
            null_count=0,
            unique_count=0,
        )
        assert profile.null_percentage == 0.0

    def test_unique_percentage_with_values(self) -> None:
        """Test unique_percentage property calculation with values."""
        profile = ColumnProfile(
            name="test_col",
            inferred_type="string",
            total_count=100,
            null_count=0,
            unique_count=50,
        )
        assert profile.unique_percentage == 50.0

    def test_unique_percentage_all_unique(self) -> None:
        """Test unique_percentage property with all unique values."""
        profile = ColumnProfile(
            name="test_col",
            inferred_type="string",
            total_count=100,
            null_count=0,
            unique_count=100,
        )
        assert profile.unique_percentage == 100.0

    def test_unique_percentage_no_unique(self) -> None:
        """Test unique_percentage property with no unique values."""
        profile = ColumnProfile(
            name="test_col",
            inferred_type="string",
            total_count=100,
            null_count=0,
            unique_count=1,
        )
        assert profile.unique_percentage == 1.0

    def test_unique_percentage_empty_column(self) -> None:
        """Test unique_percentage property with empty column."""
        profile = ColumnProfile(
            name="test_col",
            inferred_type="string",
            total_count=0,
            null_count=0,
            unique_count=0,
        )
        assert profile.unique_percentage == 0.0

    def test_default_values(self) -> None:
        """Test ColumnProfile optional field defaults."""
        profile = ColumnProfile(
            name="test_col",
            inferred_type="string",
            total_count=0,
            null_count=0,
            unique_count=0,
        )

        assert profile.name == "test_col"
        assert profile.inferred_type == "string"
        assert profile.total_count == 0
        assert profile.null_count == 0
        assert profile.unique_count == 0
        assert profile.min_value is None
        assert profile.max_value is None
        assert profile.mean_value is None
        assert profile.min_length is None
        assert profile.max_length is None
        assert profile.avg_length is None
        assert profile.detected_patterns == []
        assert profile.sample_values == []


class TestColumnProfiler:
    """Tests for ColumnProfiler class."""

    @pytest.fixture
    def profiler(self) -> ColumnProfiler:
        """Create a ColumnProfiler instance."""
        return ColumnProfiler()

    def test_init_default_treat_empty_as_null(self) -> None:
        """Test __init__ defaults to treat_empty_as_null=True."""
        profiler = ColumnProfiler()
        assert profiler._treat_empty_as_null is True

    def test_init_accept_treat_empty_as_null_false(self) -> None:
        """Test __init__ accepts treat_empty_as_null=False."""
        profiler = ColumnProfiler(treat_empty_as_null=False)
        assert profiler._treat_empty_as_null is False

    def test_is_null_returns_true_for_none(self, profiler: ColumnProfiler) -> None:
        """Test _is_null returns True for None."""
        assert profiler._is_null(None) is True

    def test_is_null_returns_true_for_empty_string_by_default(
        self, profiler: ColumnProfiler
    ) -> None:
        """Test _is_null returns True for empty string when treat_empty_as_null=True."""
        assert profiler._is_null("") is True

    def test_is_null_returns_false_for_empty_string_when_disabled(self) -> None:
        """Test _is_null returns False for empty string when treat_empty_as_null=False."""
        profiler = ColumnProfiler(treat_empty_as_null=False)
        assert profiler._is_null("") is False

    def test_is_null_returns_false_for_non_null_values(
        self, profiler: ColumnProfiler
    ) -> None:
        """Test _is_null returns False for actual values."""
        assert profiler._is_null("value") is False
        assert profiler._is_null(0) is False
        assert profiler._is_null(False) is False

    def test_profile_empty_string_counted_as_null_by_default(self) -> None:
        """Test empty strings count as null when treat_empty_as_null=True (default)."""
        profiler = ColumnProfiler()
        data = [
            {"name": "Alice"},
            {"name": ""},
            {"name": "Bob"},
        ]

        result = profiler.profile(data)

        assert result["name"].total_count == 3
        assert result["name"].null_count == 1  # Empty string counts as null
        assert result["name"].unique_count == 2  # Only "Alice" and "Bob"

    def test_profile_empty_string_not_counted_as_null_when_disabled(self) -> None:
        """Test empty strings don't count as null when treat_empty_as_null=False."""
        profiler = ColumnProfiler(treat_empty_as_null=False)
        data = [
            {"name": "Alice"},
            {"name": ""},
            {"name": "Bob"},
        ]

        result = profiler.profile(data)

        assert result["name"].total_count == 3
        assert result["name"].null_count == 0  # Empty string is not null
        assert result["name"].unique_count == 3  # "Alice", "", and "Bob"

    def test_profile_empty_data(self, profiler: ColumnProfiler) -> None:
        """Test profile() with empty data returns empty dict."""
        result = profiler.profile([])
        assert result == {}

    def test_profile_with_sample_data(self, profiler: ColumnProfiler) -> None:
        """Test profile() with sample data profiles all columns."""
        data = [
            {"name": "Alice", "age": 30, "email": "alice@example.com"},
            {"name": "Bob", "age": 25, "email": "bob@example.com"},
            {"name": "Charlie", "age": 35, "email": "charlie@example.com"},
        ]

        result = profiler.profile(data)

        assert len(result) == 3
        assert "name" in result
        assert "age" in result
        assert "email" in result

        # Check basic properties
        assert result["name"].total_count == 3
        assert result["age"].total_count == 3
        assert result["email"].total_count == 3

    def test_infer_type_string(self, profiler: ColumnProfiler) -> None:
        """Test _infer_type correctly identifies string type."""
        values = ["Alice", "Bob", "Charlie", "David"]
        inferred_type = profiler._infer_type(values)
        assert inferred_type == "string"

    def test_infer_type_number_int(self, profiler: ColumnProfiler) -> None:
        """Test _infer_type correctly identifies number type with integers."""
        values = [1, 2, 3, 4, 5]
        inferred_type = profiler._infer_type(values)
        assert inferred_type == "number"

    def test_infer_type_number_float(self, profiler: ColumnProfiler) -> None:
        """Test _infer_type correctly identifies number type with floats."""
        values = [1.5, 2.7, 3.2, 4.9]
        inferred_type = profiler._infer_type(values)
        assert inferred_type == "number"

    def test_infer_type_datetime(self, profiler: ColumnProfiler) -> None:
        """Test _infer_type correctly identifies datetime type."""
        values = [
            datetime(2024, 1, 1),
            datetime(2024, 1, 2),
            datetime(2024, 1, 3),
        ]
        inferred_type = profiler._infer_type(values)
        assert inferred_type == "datetime"

    def test_infer_type_boolean(self, profiler: ColumnProfiler) -> None:
        """Test _infer_type correctly identifies boolean type."""
        values = [True, False, True, True, False]
        inferred_type = profiler._infer_type(values)
        assert inferred_type == "boolean"

    def test_infer_type_mixed_prefers_most_common(self, profiler: ColumnProfiler) -> None:
        """Test _infer_type with mixed types returns most common."""
        # Mostly numbers with some strings
        values = [1, 2, 3, 4, 5, "text"]
        inferred_type = profiler._infer_type(values)
        assert inferred_type == "number"

    def test_is_numeric_with_bool(self, profiler: ColumnProfiler) -> None:
        """Test _is_numeric returns False for boolean values."""
        # Booleans are not numeric even though they subclass int in Python
        assert profiler._is_numeric(True) is False
        assert profiler._is_numeric(False) is False

    def test_is_numeric_with_int(self, profiler: ColumnProfiler) -> None:
        """Test _is_numeric with integer."""
        assert profiler._is_numeric(42) is True

    def test_is_numeric_with_float(self, profiler: ColumnProfiler) -> None:
        """Test _is_numeric with float."""
        assert profiler._is_numeric(42.5) is True

    def test_is_numeric_with_string_number(self, profiler: ColumnProfiler) -> None:
        """Test _is_numeric with string representation of number."""
        assert profiler._is_numeric("42") is True
        assert profiler._is_numeric("42.5") is True

    def test_is_numeric_with_comma_separated(self, profiler: ColumnProfiler) -> None:
        """Test _is_numeric with comma-separated numbers (not supported)."""
        # The implementation doesn't support comma-separated numbers
        assert profiler._is_numeric("1,234") is False
        assert profiler._is_numeric("1,234.56") is False

    def test_is_numeric_with_currency(self, profiler: ColumnProfiler) -> None:
        """Test _is_numeric with currency symbols (not supported)."""
        # The implementation doesn't support currency-prefixed numbers
        assert profiler._is_numeric("$42.50") is False
        assert profiler._is_numeric("$1,234.56") is False

    def test_is_numeric_with_non_numeric(self, profiler: ColumnProfiler) -> None:
        """Test _is_numeric with non-numeric values."""
        assert profiler._is_numeric("hello") is False
        assert profiler._is_numeric("abc123") is False

    def test_is_datetime_with_datetime_object(self, profiler: ColumnProfiler) -> None:
        """Test _is_datetime with datetime object."""
        assert profiler._is_datetime(datetime(2024, 1, 1)) is True

    def test_is_datetime_with_iso_string(self, profiler: ColumnProfiler) -> None:
        """Test _is_datetime with ISO format string."""
        assert profiler._is_datetime("2024-01-01") is True
        assert profiler._is_datetime("2024-01-01T12:00:00") is True
        assert profiler._is_datetime("2024-12-05T14:30:00Z") is True

    def test_is_datetime_with_non_datetime(self, profiler: ColumnProfiler) -> None:
        """Test _is_datetime with non-datetime values."""
        assert profiler._is_datetime("hello") is False
        assert profiler._is_datetime("123") is False
        assert profiler._is_datetime(42) is False

    def test_detect_patterns_email(self, profiler: ColumnProfiler) -> None:
        """Test _detect_patterns detects email pattern when >50% match."""
        values = [
            "alice@example.com",
            "bob@example.com",
            "charlie@test.org",
            "david@company.co.uk",
        ]
        patterns = profiler._detect_patterns(values)
        assert "email" in patterns

    def test_detect_patterns_email_insufficient_matches(self, profiler: ColumnProfiler) -> None:
        """Test _detect_patterns does not detect email when <50% match."""
        values = [
            "alice@example.com",
            "not an email",
            "also not",
            "still not",
        ]
        patterns = profiler._detect_patterns(values)
        assert "email" not in patterns

    def test_detect_patterns_phone(self, profiler: ColumnProfiler) -> None:
        """Test _detect_patterns detects phone pattern."""
        values = [
            "123-456-7890",
            "(123) 456-7890",
            "+1 123 456 7890",
            "1234567890",
        ]
        patterns = profiler._detect_patterns(values)
        assert "phone" in patterns

    def test_detect_patterns_currency(self, profiler: ColumnProfiler) -> None:
        """Test _detect_patterns detects currency pattern."""
        values = [
            "$100.00",
            "$1,234.56",
            "$50",
            "$999.99",
        ]
        patterns = profiler._detect_patterns(values)
        assert "currency" in patterns

    def test_detect_patterns_uuid(self, profiler: ColumnProfiler) -> None:
        """Test _detect_patterns detects uuid pattern."""
        values = [
            "550e8400-e29b-41d4-a716-446655440000",
            "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
            "123e4567-e89b-12d3-a456-426614174000",
        ]
        patterns = profiler._detect_patterns(values)
        assert "uuid" in patterns

    def test_detect_patterns_url(self, profiler: ColumnProfiler) -> None:
        """Test _detect_patterns detects url pattern."""
        values = [
            "https://example.com",
            "http://test.org",
            "https://www.company.com/page",
            "http://site.net/path",
        ]
        patterns = profiler._detect_patterns(values)
        assert "url" in patterns

    def test_detect_patterns_no_matches(self, profiler: ColumnProfiler) -> None:
        """Test _detect_patterns with no matches returns empty list."""
        values = ["random", "text", "values", "here"]
        patterns = profiler._detect_patterns(values)
        assert patterns == []

    def test_detect_patterns_multiple(self, profiler: ColumnProfiler) -> None:
        """Test _detect_patterns can detect multiple patterns."""
        # ISO dates that also match date_iso pattern
        values = [
            "2024-01-01",
            "2024-02-15",
            "2024-03-20",
        ]
        patterns = profiler._detect_patterns(values)
        assert "date_iso" in patterns

    def test_profile_captures_sample_values(self, profiler: ColumnProfiler) -> None:
        """Test profile captures sample_values correctly."""
        data = [
            {"name": "Alice"},
            {"name": "Bob"},
            {"name": "Charlie"},
            {"name": "David"},
            {"name": "Eve"},
        ]

        result = profiler.profile(data, sample_size=3)
        profile = result["name"]

        assert len(profile.sample_values) <= 3
        assert "Alice" in profile.sample_values

    def test_profile_calculates_string_stats(self, profiler: ColumnProfiler) -> None:
        """Test profile calculates string stats correctly."""
        data = [
            {"text": "a"},
            {"text": "abc"},
            {"text": "abcde"},
        ]

        result = profiler.profile(data)
        profile = result["text"]

        assert profile.inferred_type == "string"
        assert profile.min_length == 1
        assert profile.max_length == 5
        assert profile.avg_length == pytest.approx(3.0)

    def test_profile_calculates_numeric_stats(self, profiler: ColumnProfiler) -> None:
        """Test profile calculates numeric stats correctly."""
        data = [
            {"value": 10},
            {"value": 20},
            {"value": 30},
        ]

        result = profiler.profile(data)
        profile = result["value"]

        assert profile.inferred_type == "number"
        assert profile.min_value == 10.0
        assert profile.max_value == 30.0
        assert profile.mean_value == pytest.approx(20.0)

    def test_profile_handles_mixed_types(self, profiler: ColumnProfiler) -> None:
        """Test profile handles mixed types gracefully."""
        data = [
            {"mixed": 1},
            {"mixed": "text"},
            {"mixed": 3.5},
            {"mixed": "more text"},
        ]

        result = profiler.profile(data)
        profile = result["mixed"]

        # Should infer type based on most common
        assert profile.inferred_type in ["number", "string"]
        assert profile.total_count == 4

    def test_profile_handles_null_values(self, profiler: ColumnProfiler) -> None:
        """Test profile handles null/empty values correctly."""
        data = [
            {"value": 10},
            {"value": None},
            {"value": 20},
            {"value": None},
            {"value": 30},
        ]

        result = profiler.profile(data)
        profile = result["value"]

        assert profile.total_count == 5
        assert profile.null_count == 2
        assert profile.null_percentage == 40.0

    def test_profile_handles_missing_keys(self, profiler: ColumnProfiler) -> None:
        """Test profile handles records with missing keys."""
        data = [
            {"a": 1, "b": 2},
            {"a": 3},  # Missing 'b'
            {"b": 4},  # Missing 'a'
        ]

        result = profiler.profile(data)

        # Values are extracted using row.get(column), so all rows contribute
        # row.get() returns None for missing keys
        assert result["a"].total_count == 3  # All 3 rows
        assert result["a"].null_count == 1  # 1 row has missing 'a'
        assert result["b"].total_count == 3  # All 3 rows
        assert result["b"].null_count == 1  # 1 row has missing 'b'

    def test_profile_counts_unique_values(self, profiler: ColumnProfiler) -> None:
        """Test profile counts unique values correctly."""
        data = [
            {"status": "active"},
            {"status": "active"},
            {"status": "inactive"},
            {"status": "active"},
        ]

        result = profiler.profile(data)
        profile = result["status"]

        assert profile.unique_count == 2  # 'active' and 'inactive'
        assert profile.unique_percentage == 50.0

    def test_profile_with_datetime_column(self, profiler: ColumnProfiler) -> None:
        """Test profile correctly handles datetime columns."""
        data = [
            {"created_at": datetime(2024, 1, 1)},
            {"created_at": datetime(2024, 1, 2)},
            {"created_at": datetime(2024, 1, 3)},
        ]

        result = profiler.profile(data)
        profile = result["created_at"]

        assert profile.inferred_type == "datetime"
        assert profile.total_count == 3

    def test_profile_with_boolean_column(self, profiler: ColumnProfiler) -> None:
        """Test profile correctly handles boolean columns."""
        data = [
            {"is_active": True},
            {"is_active": False},
            {"is_active": True},
        ]

        result = profiler.profile(data)
        profile = result["is_active"]

        assert profile.inferred_type == "boolean"
        assert profile.total_count == 3

    def test_profile_respects_sample_size(self, profiler: ColumnProfiler) -> None:
        """Test profile respects sample_size parameter."""
        data = [{"id": i} for i in range(20)]

        result = profiler.profile(data, sample_size=5)
        profile = result["id"]

        assert len(profile.sample_values) <= 5

    def test_profile_with_string_numbers(self, profiler: ColumnProfiler) -> None:
        """Test profile identifies string numbers as numeric."""
        data = [
            {"amount": "100.50"},
            {"amount": "200.75"},
            {"amount": "300.00"},
        ]

        result = profiler.profile(data)
        profile = result["amount"]

        assert profile.inferred_type == "number"
        assert profile.min_value == pytest.approx(100.50)
        assert profile.max_value == pytest.approx(300.00)

    def test_profile_with_currency_strings(self, profiler: ColumnProfiler) -> None:
        """Test profile handles currency strings."""
        data = [
            {"price": "$100.00"},
            {"price": "$200.50"},
            {"price": "$150.75"},
        ]

        result = profiler.profile(data)
        profile = result["price"]

        # Currency strings are detected as strings (not numeric)
        # because _is_numeric doesn't handle $ prefix
        assert profile.inferred_type == "string"
        # Should detect currency pattern
        assert "currency" in profile.detected_patterns

    def test_profile_empty_column_values(self, profiler: ColumnProfiler) -> None:
        """Test profile with column that exists but has all None values."""
        data = [
            {"empty": None},
            {"empty": None},
            {"empty": None},
        ]

        result = profiler.profile(data)
        profile = result["empty"]

        assert profile.total_count == 3
        assert profile.null_count == 3
        assert profile.null_percentage == 100.0

    def test_profile_sparse_columns(self, profiler: ColumnProfiler) -> None:
        """Test profile with sparse columns (many missing values)."""
        data = [
            {"optional": "value1"},
            {},
            {},
            {"optional": "value2"},
            {},
        ]

        result = profiler.profile(data)

        # All rows are checked for the column, row.get() returns None for missing
        assert result["optional"].total_count == 5
        assert result["optional"].null_count == 3  # 3 rows don't have the key
