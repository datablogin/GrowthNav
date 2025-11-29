"""Tests for QueryValidator."""

import pytest
from growthnav.bigquery.validation import (
    QueryValidator,
    ValidationResult,
    ValidationSeverity,
)


class TestQueryValidator:
    """Test suite for QueryValidator."""

    @pytest.mark.parametrize(
        "sql,expected_message",
        [
            ("DROP TABLE foo", "DROP statements are not allowed"),
            ("DELETE FROM foo WHERE id=1", "DELETE statements are not allowed"),
            ("TRUNCATE TABLE foo", "TRUNCATE statements are not allowed"),
            ("UPDATE foo SET bar=1", "UPDATE statements are not allowed"),
            ("INSERT INTO foo VALUES (1)", "INSERT statements are not allowed"),
            ("MERGE INTO foo USING bar", "MERGE statements are not allowed"),
            ("CREATE TABLE foo (id INT64)", "CREATE statements are not allowed"),
            ("ALTER TABLE foo ADD COLUMN bar", "ALTER statements are not allowed"),
            ("GRANT SELECT ON foo TO user", "GRANT statements are not allowed"),
            ("REVOKE SELECT ON foo FROM user", "REVOKE statements are not allowed"),
        ],
    )
    def test_blocked_patterns_raise_error(self, sql: str, expected_message: str):
        """Test that blocked SQL patterns raise ValueError."""
        with pytest.raises(ValueError) as exc_info:
            QueryValidator.validate(sql)
        assert expected_message in str(exc_info.value)

    @pytest.mark.parametrize(
        "sql",
        [
            "DROP TABLE foo",
            "DELETE FROM foo WHERE id=1",
            "TRUNCATE TABLE foo",
            "UPDATE foo SET bar=1",
            "INSERT INTO foo VALUES (1)",
            "MERGE INTO foo USING bar ON foo.id=bar.id",
        ],
    )
    def test_allow_writes_permits_write_operations(self, sql: str):
        """Test that allow_writes=True allows write operations."""
        result = QueryValidator.validate(sql, allow_writes=True)
        assert result.is_valid is True

    def test_safe_select_query_is_valid(self):
        """Test that safe SELECT queries are valid."""
        result = QueryValidator.validate("SELECT id, name FROM customers LIMIT 10")
        assert result.is_valid is True
        assert result.severity is None

    def test_select_star_produces_warning(self):
        """Test that SELECT * produces a warning."""
        result = QueryValidator.validate("SELECT * FROM customers LIMIT 10")
        assert result.is_valid is True
        assert result.severity == ValidationSeverity.WARNING
        assert "SELECT *" in result.message

    def test_missing_limit_produces_warning(self):
        """Test that missing LIMIT produces a warning."""
        result = QueryValidator.validate("SELECT id, name FROM customers")
        assert result.is_valid is True
        assert result.severity == ValidationSeverity.WARNING
        assert "LIMIT" in result.message

    def test_select_star_without_limit_produces_multiple_warnings(self):
        """Test that SELECT * without LIMIT produces multiple warnings."""
        result = QueryValidator.validate("SELECT * FROM customers")
        assert result.is_valid is True
        assert result.severity == ValidationSeverity.WARNING
        # Should contain both warnings
        assert "SELECT *" in result.message
        assert "LIMIT" in result.message

    def test_sanitize_identifier_valid(self):
        """Test sanitize_identifier with valid identifiers."""
        # Valid identifiers
        assert QueryValidator.sanitize_identifier("customer_id") == "customer_id"
        assert QueryValidator.sanitize_identifier("table_name") == "table_name"
        assert QueryValidator.sanitize_identifier("_private") == "_private"
        assert QueryValidator.sanitize_identifier("col123") == "col123"
        assert QueryValidator.sanitize_identifier("dataset-name") == "dataset-name"

    @pytest.mark.parametrize(
        "invalid_identifier",
        [
            "123invalid",  # Starts with number
            "col name",  # Contains space
            "col;name",  # Contains semicolon
            "col'name",  # Contains quote
            "col.name",  # Contains dot
            "",  # Empty string
        ],
    )
    def test_sanitize_identifier_invalid(self, invalid_identifier: str):
        """Test sanitize_identifier with invalid identifiers."""
        with pytest.raises(ValueError) as exc_info:
            QueryValidator.sanitize_identifier(invalid_identifier)
        assert "Invalid identifier" in str(exc_info.value)

    def test_validate_project_dataset_valid(self):
        """Test validate_project_dataset with valid inputs."""
        # Valid project and dataset
        assert QueryValidator.validate_project_dataset(
            "my-project-123", "my_dataset"
        ) is True
        assert QueryValidator.validate_project_dataset(
            "growthnav-prod", "growthnav_topgolf"
        ) is True

    @pytest.mark.parametrize(
        "project_id,dataset",
        [
            ("Project", "dataset"),  # Project has uppercase
            ("p", "dataset"),  # Project too short (< 6 chars)
            ("p" * 31, "dataset"),  # Project too long (> 30 chars)
            ("my_project", "dataset"),  # Project has underscore
            ("my-project", "123invalid"),  # Dataset starts with number
            ("my-project", "data set"),  # Dataset has space
            ("my-project", "data-set"),  # Dataset has hyphen
        ],
    )
    def test_validate_project_dataset_invalid(self, project_id: str, dataset: str):
        """Test validate_project_dataset with invalid inputs."""
        with pytest.raises(ValueError) as exc_info:
            QueryValidator.validate_project_dataset(project_id, dataset)
        assert "Invalid" in str(exc_info.value)

    def test_case_insensitive_validation(self):
        """Test that validation is case-insensitive."""
        # Should block regardless of case
        with pytest.raises(ValueError):
            QueryValidator.validate("drop table foo")
        with pytest.raises(ValueError):
            QueryValidator.validate("DrOp TaBlE foo")

    def test_validation_result_structure(self):
        """Test ValidationResult dataclass structure."""
        # Valid query returns result with no severity/message
        result = QueryValidator.validate("SELECT id FROM foo LIMIT 10")
        assert isinstance(result, ValidationResult)
        assert result.is_valid is True
        assert result.severity is None
        assert result.message is None

        # Warning query returns result with severity and message
        result = QueryValidator.validate("SELECT * FROM foo LIMIT 10")
        assert isinstance(result, ValidationResult)
        assert result.is_valid is True
        assert result.severity == ValidationSeverity.WARNING
        assert result.message is not None
