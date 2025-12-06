"""Column profiling for schema discovery."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class ColumnProfile:
    """Profile of a single column including type, statistics, and patterns.

    Attributes:
        name: Column name
        inferred_type: Inferred data type (string, number, datetime, boolean, unknown)
        total_count: Total number of values (including nulls)
        null_count: Number of null/None values
        unique_count: Number of unique values
        min_value: Minimum value for numeric columns
        max_value: Maximum value for numeric columns
        mean_value: Mean value for numeric columns
        min_length: Minimum string length for string columns
        max_length: Maximum string length for string columns
        avg_length: Average string length for string columns
        detected_patterns: List of detected patterns (email, phone, etc.)
        sample_values: Sample of actual values from the column
    """

    name: str
    inferred_type: str
    total_count: int
    null_count: int
    unique_count: int
    min_value: float | None = None
    max_value: float | None = None
    mean_value: float | None = None
    min_length: int | None = None
    max_length: int | None = None
    avg_length: float | None = None
    detected_patterns: list[str] = field(default_factory=list)
    sample_values: list[Any] = field(default_factory=list)

    @property
    def null_percentage(self) -> float:
        """Return percentage of null values."""
        if self.total_count == 0:
            return 0.0
        return (self.null_count / self.total_count) * 100

    @property
    def unique_percentage(self) -> float:
        """Return percentage of unique values."""
        if self.total_count == 0:
            return 0.0
        return (self.unique_count / self.total_count) * 100


class ColumnProfiler:
    """Profile columns in datasets to infer types, detect patterns, and gather statistics.

    Example:
        profiler = ColumnProfiler()
        data = [
            {"email": "user@example.com", "age": 25, "active": True},
            {"email": "admin@example.com", "age": 30, "active": False},
        ]
        profiles = profiler.profile(data)
        print(profiles["email"].detected_patterns)  # ["email"]
        print(profiles["age"].inferred_type)  # "number"
    """

    # Regex patterns for common data formats (compiled for performance)
    PATTERNS = {
        "email": re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"),
        "phone": re.compile(
            r"^[\+]?[(]?[0-9]{1,4}[)]?[-\s\.]?[(]?[0-9]{1,4}[)]?[-\s\.]?[0-9]{1,9}$"
        ),
        "currency": re.compile(r"^\$?\d+(\.\d{2})?$"),
        "date_iso": re.compile(r"^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}:\d{2})?"),
        "uuid": re.compile(
            r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
            re.IGNORECASE,
        ),
        "gclid": re.compile(r"^[A-Za-z0-9_-]{20,}$"),
        "url": re.compile(r"^https?://[^\s/$.?#].[^\s]*$"),
    }

    def profile(
        self, data: list[dict], sample_size: int = 10
    ) -> dict[str, ColumnProfile]:
        """Profile all columns in a dataset.

        Args:
            data: List of dictionaries representing rows
            sample_size: Number of sample values to store per column

        Returns:
            Dictionary mapping column names to their profiles
        """
        if not data:
            return {}

        # Extract all unique column names
        columns: set[str] = set()
        for row in data:
            columns.update(row.keys())

        # Profile each column
        profiles = {}
        for column in columns:
            values = [row.get(column) for row in data]
            profiles[column] = self._profile_column(column, values, sample_size)

        return profiles

    def _profile_column(
        self, name: str, values: list[Any], sample_size: int
    ) -> ColumnProfile:
        """Profile a single column.

        Args:
            name: Column name
            values: List of all values in the column
            sample_size: Number of sample values to store

        Returns:
            ColumnProfile for the column
        """
        total_count = len(values)
        null_count = sum(1 for v in values if v is None or v == "")
        non_null_values = [v for v in values if v is not None and v != ""]
        unique_count = len(set(str(v) for v in non_null_values))

        # Infer type from first 100 non-null values for performance
        sample_for_inference = non_null_values[:100]
        inferred_type = self._infer_type(sample_for_inference)

        # Initialize profile
        profile = ColumnProfile(
            name=name,
            inferred_type=inferred_type,
            total_count=total_count,
            null_count=null_count,
            unique_count=unique_count,
        )

        # Compute type-specific statistics
        if inferred_type == "number" and non_null_values:
            numeric_values = [
                float(v) for v in non_null_values if self._is_numeric(v)
            ]
            if numeric_values:
                profile.min_value = min(numeric_values)
                profile.max_value = max(numeric_values)
                profile.mean_value = sum(numeric_values) / len(numeric_values)

        elif inferred_type == "string" and non_null_values:
            str_values = [str(v) for v in non_null_values]
            lengths = [len(s) for s in str_values]
            profile.min_length = min(lengths)
            profile.max_length = max(lengths)
            profile.avg_length = sum(lengths) / len(lengths)

        # Detect patterns
        profile.detected_patterns = self._detect_patterns(non_null_values)

        # Store sample values
        profile.sample_values = non_null_values[:sample_size]

        return profile

    def _infer_type(self, values: list[Any]) -> str:
        """Infer the type of a column from sample values.

        Args:
            values: Sample of non-null values

        Returns:
            Inferred type: "string", "number", "datetime", "boolean", or "unknown"
        """
        if not values:
            return "unknown"

        # Count how many values match each type
        type_counts = {
            "boolean": 0,
            "number": 0,
            "datetime": 0,
            "string": 0,
        }

        for value in values:
            if isinstance(value, bool):
                type_counts["boolean"] += 1
            elif self._is_numeric(value):
                type_counts["number"] += 1
            elif self._is_datetime(value):
                type_counts["datetime"] += 1
            else:
                type_counts["string"] += 1

        # Return type with highest count (>50% threshold)
        total = len(values)
        for type_name, count in type_counts.items():
            if count / total > 0.5:
                return type_name

        return "string"  # Default to string if no clear majority

    def _is_numeric(self, value: Any) -> bool:
        """Check if a value can be interpreted as numeric.

        Args:
            value: Value to check

        Returns:
            True if value is numeric
        """
        if isinstance(value, bool):
            return False
        if isinstance(value, (int, float)):
            return True
        if isinstance(value, str):
            try:
                float(value)
                return True
            except (ValueError, TypeError):
                return False
        return False

    def _is_datetime(self, value: Any) -> bool:
        """Check if a value can be interpreted as a datetime.

        Args:
            value: Value to check

        Returns:
            True if value is a datetime
        """
        if isinstance(value, datetime):
            return True
        if isinstance(value, str):
            # Try common datetime formats
            datetime_patterns = [
                r"^\d{4}-\d{2}-\d{2}$",  # YYYY-MM-DD
                r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}",  # ISO 8601
                r"^\d{2}/\d{2}/\d{4}$",  # MM/DD/YYYY
                r"^\d{4}/\d{2}/\d{2}$",  # YYYY/MM/DD
            ]
            return any(re.match(pattern, value) for pattern in datetime_patterns)
        return False

    def _detect_patterns(self, values: list[Any]) -> list[str]:
        """Detect common patterns in values.

        A pattern is detected if >50% of values match it.

        Args:
            values: List of non-null values

        Returns:
            List of detected pattern names
        """
        if not values:
            return []

        total = len(values)
        threshold = 0.5

        # Count matches for each pattern in a single pass through values
        pattern_counts: dict[str, int] = {name: 0 for name in self.PATTERNS}

        for value in values:
            if isinstance(value, str):
                for pattern_name, pattern_regex in self.PATTERNS.items():
                    if pattern_regex.match(value):
                        pattern_counts[pattern_name] += 1

        # Return patterns that exceed threshold
        return [
            name for name, count in pattern_counts.items() if count / total > threshold
        ]
