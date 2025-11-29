"""
QueryValidator - SQL query validation to prevent destructive operations.

Blocks:
- DROP, DELETE, TRUNCATE, UPDATE, INSERT (unless explicitly allowed)
- DDL statements
- Suspicious patterns
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum


class ValidationSeverity(str, Enum):
    """Severity level for validation issues."""

    ERROR = "error"  # Query blocked
    WARNING = "warning"  # Query allowed with warning


@dataclass
class ValidationResult:
    """Result of query validation."""

    is_valid: bool
    severity: ValidationSeverity | None = None
    message: str | None = None


class QueryValidator:
    """
    Validates SQL queries for safety.

    Prevents:
    - Data modification (DELETE, UPDATE, INSERT, MERGE)
    - Schema changes (DROP, CREATE, ALTER, TRUNCATE)
    - Dangerous patterns (wildcards, no LIMIT)
    """

    # Blocked statement patterns
    BLOCKED_PATTERNS = [
        (r"\bDROP\s+", "DROP statements are not allowed"),
        (r"\bDELETE\s+", "DELETE statements are not allowed"),
        (r"\bTRUNCATE\s+", "TRUNCATE statements are not allowed"),
        (r"\bUPDATE\s+", "UPDATE statements are not allowed"),
        (r"\bINSERT\s+", "INSERT statements are not allowed"),
        (r"\bMERGE\s+", "MERGE statements are not allowed"),
        (r"\bCREATE\s+", "CREATE statements are not allowed"),
        (r"\bALTER\s+", "ALTER statements are not allowed"),
        (r"\bGRANT\s+", "GRANT statements are not allowed"),
        (r"\bREVOKE\s+", "REVOKE statements are not allowed"),
    ]

    # Warning patterns
    WARNING_PATTERNS = [
        (r"SELECT\s+\*", "Consider specifying columns instead of SELECT *"),
        (r"(?<!LIMIT\s)\d{5,}", "Large numbers detected - verify intent"),
    ]

    # Recommended patterns
    RECOMMENDED_PATTERNS = [
        r"\bLIMIT\s+\d+",  # Has LIMIT clause
    ]

    @classmethod
    def validate(cls, sql: str, allow_writes: bool = False) -> ValidationResult:
        """
        Validate a SQL query for safety.

        Args:
            sql: SQL query string
            allow_writes: If True, allow write operations (for admin use)

        Returns:
            ValidationResult with status and message

        Raises:
            ValueError: If query contains blocked patterns
        """
        sql_upper = sql.upper()

        # Check blocked patterns
        if not allow_writes:
            for pattern, message in cls.BLOCKED_PATTERNS:
                if re.search(pattern, sql_upper, re.IGNORECASE):
                    raise ValueError(f"Query validation failed: {message}")

        # Check for warnings
        warnings = []
        for pattern, message in cls.WARNING_PATTERNS:
            if re.search(pattern, sql_upper, re.IGNORECASE):
                warnings.append(message)

        # Check recommended patterns
        has_limit = any(
            re.search(pattern, sql_upper, re.IGNORECASE)
            for pattern in cls.RECOMMENDED_PATTERNS
        )

        if not has_limit and "SELECT" in sql_upper:
            warnings.append("Consider adding a LIMIT clause to prevent large result sets")

        if warnings:
            return ValidationResult(
                is_valid=True,
                severity=ValidationSeverity.WARNING,
                message="; ".join(warnings),
            )

        return ValidationResult(is_valid=True)

    @classmethod
    def sanitize_identifier(cls, identifier: str) -> str:
        """
        Sanitize a table/column identifier to prevent injection.

        Args:
            identifier: Table or column name

        Returns:
            Sanitized identifier

        Raises:
            ValueError: If identifier contains invalid characters
        """
        # Only allow alphanumeric, underscore, and hyphen
        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_-]*$", identifier):
            raise ValueError(f"Invalid identifier: {identifier}")

        return identifier

    @classmethod
    def validate_project_dataset(cls, project_id: str, dataset: str) -> bool:
        """
        Validate project and dataset identifiers.

        Args:
            project_id: GCP project ID
            dataset: BigQuery dataset name

        Returns:
            True if valid

        Raises:
            ValueError: If identifiers are invalid
        """
        # Project ID: lowercase letters, digits, hyphens
        if not re.match(r"^[a-z][a-z0-9-]{5,29}$", project_id):
            raise ValueError(f"Invalid project ID: {project_id}")

        # Dataset: letters, digits, underscores
        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]{0,1023}$", dataset):
            raise ValueError(f"Invalid dataset: {dataset}")

        return True
