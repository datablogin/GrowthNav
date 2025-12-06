"""Tests for growthnav.connectors.identity module."""

from __future__ import annotations

import sys
from typing import Any

import pytest
from growthnav.connectors.identity import (
    IdentityFragment,
    IdentityLinker,
    IdentityType,
    ResolvedIdentity,
)


class TestIdentityType:
    """Tests for IdentityType enum."""

    def test_all_enum_values_exist(self) -> None:
        """Test all expected identity types are defined."""
        assert IdentityType.EMAIL == "email"
        assert IdentityType.PHONE == "phone"
        assert IdentityType.HASHED_CC == "hashed_cc"
        assert IdentityType.LOYALTY_ID == "loyalty_id"
        assert IdentityType.DEVICE_ID == "device_id"
        assert IdentityType.COOKIE_ID == "cookie_id"
        assert IdentityType.CUSTOMER_ID == "customer_id"
        assert IdentityType.FULL_NAME == "full_name"

    def test_string_conversion(self) -> None:
        """Test enum values convert to strings correctly."""
        assert IdentityType.EMAIL.value == "email"
        assert IdentityType.PHONE.value == "phone"
        assert IdentityType.HASHED_CC.value == "hashed_cc"

    def test_enum_count(self) -> None:
        """Test we have exactly 8 identity types."""
        assert len(IdentityType) == 8


class TestIdentityFragment:
    """Tests for IdentityFragment dataclass."""

    def test_creation_with_all_fields(self) -> None:
        """Test creating fragment with all fields."""
        fragment = IdentityFragment(
            fragment_type=IdentityType.EMAIL,
            fragment_value="jane.doe@example.com",
            source_system="olo",
            confidence=0.95,
            metadata={"source_table": "customers"},
        )

        assert fragment.fragment_type == IdentityType.EMAIL
        assert fragment.fragment_value == "jane.doe@example.com"
        assert fragment.source_system == "olo"
        assert fragment.confidence == 0.95
        assert fragment.metadata == {"source_table": "customers"}

    def test_default_values(self) -> None:
        """Test fragment creation with default values."""
        fragment = IdentityFragment(
            fragment_type=IdentityType.PHONE,
            fragment_value="5551234567",
        )

        assert fragment.source_system is None
        assert fragment.confidence == 1.0
        assert fragment.metadata == {}

    def test_hash_by_type_and_lowercased_value(self) -> None:
        """Test __hash__ hashes by type and lowercased value."""
        fragment1 = IdentityFragment(
            fragment_type=IdentityType.EMAIL,
            fragment_value="Jane.Doe@Example.COM",
        )
        fragment2 = IdentityFragment(
            fragment_type=IdentityType.EMAIL,
            fragment_value="jane.doe@example.com",
        )

        # Same hash despite different case
        assert hash(fragment1) == hash(fragment2)

    def test_hash_different_for_different_types(self) -> None:
        """Test __hash__ differs for different types."""
        fragment1 = IdentityFragment(
            fragment_type=IdentityType.EMAIL,
            fragment_value="test@example.com",
        )
        fragment2 = IdentityFragment(
            fragment_type=IdentityType.CUSTOMER_ID,
            fragment_value="test@example.com",
        )

        # Different hash for different types even with same value
        assert hash(fragment1) != hash(fragment2)

    def test_equality_case_insensitive(self) -> None:
        """Test __eq__ performs case-insensitive comparison."""
        fragment1 = IdentityFragment(
            fragment_type=IdentityType.EMAIL,
            fragment_value="Jane.Doe@Example.COM",
        )
        fragment2 = IdentityFragment(
            fragment_type=IdentityType.EMAIL,
            fragment_value="jane.doe@example.com",
        )

        assert fragment1 == fragment2

    def test_equality_same_type_and_value_different_case(self) -> None:
        """Test two fragments with same type and value (different case) are equal."""
        fragment1 = IdentityFragment(
            fragment_type=IdentityType.LOYALTY_ID,
            fragment_value="MEMBER123",
        )
        fragment2 = IdentityFragment(
            fragment_type=IdentityType.LOYALTY_ID,
            fragment_value="member123",
        )

        assert fragment1 == fragment2

    def test_inequality_different_types(self) -> None:
        """Test two fragments with different types are not equal."""
        fragment1 = IdentityFragment(
            fragment_type=IdentityType.EMAIL,
            fragment_value="test@example.com",
        )
        fragment2 = IdentityFragment(
            fragment_type=IdentityType.PHONE,
            fragment_value="test@example.com",
        )

        assert fragment1 != fragment2

    def test_equality_ignores_metadata(self) -> None:
        """Test equality ignores metadata differences."""
        fragment1 = IdentityFragment(
            fragment_type=IdentityType.EMAIL,
            fragment_value="test@example.com",
            metadata={"source": "olo"},
        )
        fragment2 = IdentityFragment(
            fragment_type=IdentityType.EMAIL,
            fragment_value="test@example.com",
            metadata={"source": "toast"},
        )

        # Should be equal despite different metadata
        assert fragment1 == fragment2

    def test_equality_ignores_confidence(self) -> None:
        """Test equality ignores confidence differences."""
        fragment1 = IdentityFragment(
            fragment_type=IdentityType.EMAIL,
            fragment_value="test@example.com",
            confidence=0.9,
        )
        fragment2 = IdentityFragment(
            fragment_type=IdentityType.EMAIL,
            fragment_value="test@example.com",
            confidence=0.5,
        )

        # Should be equal despite different confidence
        assert fragment1 == fragment2


class TestResolvedIdentity:
    """Tests for ResolvedIdentity dataclass."""

    def test_creation_with_global_id_and_fragments(self) -> None:
        """Test creating resolved identity with all fields."""
        fragments = [
            IdentityFragment(IdentityType.EMAIL, "jane@example.com"),
            IdentityFragment(IdentityType.PHONE, "5551234567"),
        ]

        identity = ResolvedIdentity(
            global_id="uuid-123",
            fragments=fragments,
            match_probability=0.95,
        )

        assert identity.global_id == "uuid-123"
        assert len(identity.fragments) == 2
        assert identity.match_probability == 0.95

    def test_default_empty_fragments_list(self) -> None:
        """Test default fragments is an empty list."""
        identity = ResolvedIdentity(global_id="uuid-456")

        assert identity.fragments == []
        assert identity.match_probability == 1.0

    def test_emails_property_returns_only_email_values(self) -> None:
        """Test emails property returns only email fragment values."""
        identity = ResolvedIdentity(
            global_id="uuid-123",
            fragments=[
                IdentityFragment(IdentityType.EMAIL, "jane@example.com"),
                IdentityFragment(IdentityType.EMAIL, "jane.doe@work.com"),
                IdentityFragment(IdentityType.PHONE, "5551234567"),
                IdentityFragment(IdentityType.LOYALTY_ID, "MEMBER123"),
            ],
        )

        emails = identity.emails

        assert len(emails) == 2
        assert "jane@example.com" in emails
        assert "jane.doe@work.com" in emails
        assert "5551234567" not in emails

    def test_emails_property_empty_when_no_email_fragments(self) -> None:
        """Test emails property returns empty list when no email fragments."""
        identity = ResolvedIdentity(
            global_id="uuid-123",
            fragments=[
                IdentityFragment(IdentityType.PHONE, "5551234567"),
                IdentityFragment(IdentityType.LOYALTY_ID, "MEMBER123"),
            ],
        )

        assert identity.emails == []

    def test_phones_property_returns_only_phone_values(self) -> None:
        """Test phones property returns only phone fragment values."""
        identity = ResolvedIdentity(
            global_id="uuid-123",
            fragments=[
                IdentityFragment(IdentityType.EMAIL, "jane@example.com"),
                IdentityFragment(IdentityType.PHONE, "5551234567"),
                IdentityFragment(IdentityType.PHONE, "5559876543"),
                IdentityFragment(IdentityType.LOYALTY_ID, "MEMBER123"),
            ],
        )

        phones = identity.phones

        assert len(phones) == 2
        assert "5551234567" in phones
        assert "5559876543" in phones
        assert "jane@example.com" not in phones

    def test_phones_property_empty_when_no_phone_fragments(self) -> None:
        """Test phones property returns empty list when no phone fragments."""
        identity = ResolvedIdentity(
            global_id="uuid-123",
            fragments=[
                IdentityFragment(IdentityType.EMAIL, "jane@example.com"),
                IdentityFragment(IdentityType.LOYALTY_ID, "MEMBER123"),
            ],
        )

        assert identity.phones == []

    def test_has_fragment_type_positive_case(self) -> None:
        """Test has_fragment_type returns True when type exists."""
        identity = ResolvedIdentity(
            global_id="uuid-123",
            fragments=[
                IdentityFragment(IdentityType.EMAIL, "jane@example.com"),
                IdentityFragment(IdentityType.PHONE, "5551234567"),
            ],
        )

        assert identity.has_fragment_type(IdentityType.EMAIL) is True
        assert identity.has_fragment_type(IdentityType.PHONE) is True

    def test_has_fragment_type_negative_case(self) -> None:
        """Test has_fragment_type returns False when type doesn't exist."""
        identity = ResolvedIdentity(
            global_id="uuid-123",
            fragments=[
                IdentityFragment(IdentityType.EMAIL, "jane@example.com"),
            ],
        )

        assert identity.has_fragment_type(IdentityType.PHONE) is False
        assert identity.has_fragment_type(IdentityType.LOYALTY_ID) is False
        assert identity.has_fragment_type(IdentityType.HASHED_CC) is False


@pytest.fixture
def sample_records_email_match() -> list[dict[str, Any]]:
    """Sample records that match by email."""
    return [
        {"id": "1", "email": "jane@example.com", "phone": "5551111111"},
        {"id": "2", "email": "jane@example.com", "phone": "5552222222"},
    ]


@pytest.fixture
def sample_records_phone_match() -> list[dict[str, Any]]:
    """Sample records that match by phone."""
    return [
        {"id": "1", "email": "jane@example.com", "phone": "555-123-4567"},
        {"id": "2", "email": "john@example.com", "phone": "(555) 123-4567"},
    ]


@pytest.fixture
def sample_records_transitive() -> list[dict[str, Any]]:
    """Sample records for transitive linking test."""
    return [
        {"id": "1", "email": "jane@example.com", "phone": "5551111111"},
        {"id": "2", "email": "jane@example.com", "phone": "5552222222"},
        {"id": "3", "email": "different@example.com", "phone": "5552222222"},
    ]


@pytest.fixture
def sample_records_no_match() -> list[dict[str, Any]]:
    """Sample records with no matches."""
    return [
        {"id": "1", "email": "jane@example.com", "phone": "5551111111"},
        {"id": "2", "email": "john@example.com", "phone": "5552222222"},
        {"id": "3", "email": "alice@example.com", "phone": "5553333333"},
    ]


class TestIdentityLinkerDeterministic:
    """Tests for IdentityLinker deterministic resolution."""

    def test_add_records_normalizes_emails_lowercase_strip(self) -> None:
        """Test add_records normalizes emails to lowercase and strips whitespace."""
        linker = IdentityLinker()

        linker.add_records(
            [
                {"id": "1", "email": "  Jane.Doe@EXAMPLE.COM  "},
                {"id": "2", "email_address": "John@Example.COM"},
            ],
            source="test",
        )

        assert len(linker._records) == 2
        assert linker._records[0]["email"] == "jane.doe@example.com"
        assert linker._records[1]["email"] == "john@example.com"

    def test_add_records_normalizes_phones_digits_only_last_10(self) -> None:
        """Test add_records normalizes phones to digits only, last 10."""
        linker = IdentityLinker()

        linker.add_records(
            [
                {"id": "1", "phone": "(555) 123-4567"},
                {"id": "2", "phone_number": "+1-555-987-6543"},
                {"id": "3", "phone": "555.111.2222"},
            ],
            source="test",
        )

        assert len(linker._records) == 3
        assert linker._records[0]["phone"] == "5551234567"
        assert linker._records[1]["phone"] == "5559876543"
        assert linker._records[2]["phone"] == "5551112222"

    def test_add_records_normalizes_names(self) -> None:
        """Test add_records normalizes names to lowercase and strips."""
        linker = IdentityLinker()

        linker.add_records(
            [
                {"id": "1", "first_name": "  Jane  ", "last_name": "DOE"},
                {"id": "2", "first_name": "John", "last_name": "  Smith  "},
            ],
            source="test",
        )

        assert len(linker._records) == 2
        assert linker._records[0]["first_name"] == "jane"
        assert linker._records[0]["last_name"] == "doe"
        assert linker._records[1]["first_name"] == "john"
        assert linker._records[1]["last_name"] == "smith"

    def test_add_records_rejects_invalid_emails(self) -> None:
        """Test add_records rejects invalid email formats."""
        linker = IdentityLinker()

        linker.add_records(
            [
                {"id": "1", "email": "not_an_email"},  # No @
                {"id": "2", "email": "@@@"},  # Multiple @ but invalid
                {"id": "3", "email": "a@b"},  # Too short
                {"id": "4", "email": "valid@example.com"},  # Valid
            ],
            source="test",
        )

        assert len(linker._records) == 4
        # Invalid emails should be empty string
        assert linker._records[0]["email"] == ""
        assert linker._records[1]["email"] == ""
        assert linker._records[2]["email"] == ""
        # Valid email should be preserved
        assert linker._records[3]["email"] == "valid@example.com"

    def test_add_records_rejects_short_phone_numbers(self) -> None:
        """Test add_records rejects phone numbers with < 10 digits."""
        linker = IdentityLinker()

        linker.add_records(
            [
                {"id": "1", "phone": "555-1234"},  # Only 7 digits
                {"id": "2", "phone": "123"},  # Only 3 digits
                {"id": "3", "phone": ""},  # Empty
                {"id": "4", "phone": "555-123-4567"},  # Valid 10 digits
            ],
            source="test",
        )

        assert len(linker._records) == 4
        # Short phone numbers should be empty string
        assert linker._records[0]["phone"] == ""
        assert linker._records[1]["phone"] == ""
        assert linker._records[2]["phone"] == ""
        # Valid phone should be preserved
        assert linker._records[3]["phone"] == "5551234567"

    def test_resolve_deterministic_links_by_exact_email_match(
        self,
        sample_records_email_match: list[dict[str, Any]],
    ) -> None:
        """Test resolve_deterministic links records with matching emails."""
        linker = IdentityLinker()
        linker.add_records(sample_records_email_match, source="test")

        identities = linker.resolve_deterministic()

        # Should have 1 identity linking both records
        assert len(identities) == 1
        assert len(identities[0].fragments) >= 2  # At least 2 fragments

        # Should have the shared email
        emails = identities[0].emails
        assert "jane@example.com" in emails

    def test_resolve_deterministic_links_by_exact_phone_match(
        self,
        sample_records_phone_match: list[dict[str, Any]],
    ) -> None:
        """Test resolve_deterministic links records with matching phones."""
        linker = IdentityLinker()
        linker.add_records(sample_records_phone_match, source="test")

        identities = linker.resolve_deterministic()

        # Should have 1 identity linking both records
        assert len(identities) == 1

        # Should have the normalized phone (digits only)
        phones = identities[0].phones
        assert "5551234567" in phones

    def test_resolve_deterministic_links_by_hashed_cc_match(self) -> None:
        """Test resolve_deterministic links records by hashed_cc match."""
        linker = IdentityLinker()

        linker.add_records(
            [
                {"id": "1", "email": "jane@example.com", "hashed_cc": "abc123"},
                {"id": "2", "email": "different@example.com", "cc_hash": "abc123"},
            ],
            source="test",
        )

        identities = linker.resolve_deterministic()

        # Should link by hashed_cc
        assert len(identities) == 1
        assert len(identities[0].emails) == 2

    def test_resolve_deterministic_links_by_loyalty_id_match(self) -> None:
        """Test resolve_deterministic links records by loyalty_id match."""
        linker = IdentityLinker()

        linker.add_records(
            [
                {"id": "1", "email": "jane@example.com", "loyalty_id": "MEMBER123"},
                {"id": "2", "email": "different@example.com", "member_id": "MEMBER123"},
            ],
            source="test",
        )

        identities = linker.resolve_deterministic()

        # Should link by loyalty_id
        assert len(identities) == 1
        assert len(identities[0].emails) == 2

    def test_resolve_deterministic_transitive_linking(
        self,
        sample_records_transitive: list[dict[str, Any]],
    ) -> None:
        """Test transitive linking: A-email-B, B-phone-C -> A,B,C in same identity."""
        linker = IdentityLinker()
        linker.add_records(sample_records_transitive, source="test")

        identities = linker.resolve_deterministic()

        # All three should be linked transitively
        # Record 1 and 2 share email
        # Record 2 and 3 share phone
        # Therefore all 3 should be in same identity
        assert len(identities) == 1

        # Should have both emails
        emails = identities[0].emails
        assert "jane@example.com" in emails
        assert "different@example.com" in emails

        # Should have both phones
        phones = identities[0].phones
        assert "5551111111" in phones
        assert "5552222222" in phones

    def test_resolve_deterministic_no_linking_when_no_matches(
        self,
        sample_records_no_match: list[dict[str, Any]],
    ) -> None:
        """Test no linking occurs when records don't match."""
        linker = IdentityLinker()
        linker.add_records(sample_records_no_match, source="test")

        identities = linker.resolve_deterministic()

        # Should have 3 separate identities
        assert len(identities) == 3

        # Each should have only 1 email
        for identity in identities:
            assert len(identity.emails) == 1

    def test_resolve_deterministic_empty_records_returns_empty_list(self) -> None:
        """Test empty records returns empty list."""
        linker = IdentityLinker()

        identities = linker.resolve_deterministic()

        assert identities == []

    def test_resolve_deterministic_handles_missing_fields(self) -> None:
        """Test resolve handles records with missing identity fields."""
        linker = IdentityLinker()

        linker.add_records(
            [
                {"id": "1"},  # No identity fields
                {"id": "2", "email": "test@example.com"},
            ],
            source="test",
        )

        identities = linker.resolve_deterministic()

        # Should create separate identities (no linking)
        # Record 1 has no identifiers, so it might be filtered or separate
        assert len(identities) >= 1

    def test_add_records_with_custom_id_column(self) -> None:
        """Test add_records with custom id_column parameter."""
        linker = IdentityLinker()

        linker.add_records(
            [
                {"customer_id": "CUST001", "email": "jane@example.com"},
                {"customer_id": "CUST002", "email": "john@example.com"},
            ],
            source="test",
            id_column="customer_id",
        )

        assert len(linker._records) == 2
        assert linker._records[0]["source_id"] == "CUST001"
        assert linker._records[1]["source_id"] == "CUST002"

    def test_add_records_multiple_sources(self) -> None:
        """Test adding records from multiple sources."""
        linker = IdentityLinker()

        linker.add_records(
            [{"id": "1", "email": "jane@example.com"}],
            source="olo",
        )

        linker.add_records(
            [{"id": "2", "email": "jane@example.com"}],
            source="toast",
        )

        assert len(linker._records) == 2
        assert linker._records[0]["source_system"] == "olo"
        assert linker._records[1]["source_system"] == "toast"

        # Should link across sources
        identities = linker.resolve_deterministic()
        assert len(identities) == 1


def _find_spec(name: str) -> Any:
    """Check if a module spec can be found."""
    try:
        import importlib.util
        return importlib.util.find_spec(name)
    except (ImportError, ModuleNotFoundError):
        return None


# Skip probabilistic tests if splink is not installed
HAS_SPLINK = "splink" in sys.modules or _find_spec("splink") is not None


@pytest.mark.skipif(not HAS_SPLINK, reason="splink not installed")
class TestIdentityLinkerProbabilistic:
    """Tests for IdentityLinker probabilistic resolution using Splink."""

    def test_resolve_runs_without_errors(self) -> None:
        """Test resolve method runs without errors (basic smoke test)."""
        linker = IdentityLinker()

        linker.add_records(
            [
                {"id": "1", "email": "jane@example.com", "first_name": "Jane", "last_name": "Doe"},
                {"id": "2", "email": "jane.doe@example.com", "first_name": "Jane", "last_name": "Doe"},
            ],
            source="test",
        )

        # Should not raise an exception
        identities = linker.resolve(match_threshold=0.7)

        # Should return list of identities
        assert isinstance(identities, list)
        assert len(identities) > 0

        # Each identity should have a global_id and fragments
        for identity in identities:
            assert isinstance(identity.global_id, str)
            assert isinstance(identity.fragments, list)
            assert identity.match_probability >= 0.0
            assert identity.match_probability <= 1.0

    def test_resolve_with_overlapping_data_across_sources(self) -> None:
        """Test resolve with overlapping data from different sources."""
        linker = IdentityLinker()

        # Add records from source 1
        linker.add_records(
            [
                {"id": "1", "email": "jane@example.com", "phone": "5551234567"},
                {"id": "2", "email": "john@example.com", "phone": "5559876543"},
            ],
            source="olo",
        )

        # Add records from source 2 with slight variations
        linker.add_records(
            [
                {"id": "A", "email": "jane.doe@example.com", "phone": "555-123-4567"},
                {"id": "B", "email": "john.smith@example.com", "phone": "555-987-6543"},
            ],
            source="toast",
        )

        identities = linker.resolve(match_threshold=0.5)

        # Should create identities
        assert len(identities) > 0

        # Should have reasonable probabilities
        for identity in identities:
            assert 0.0 <= identity.match_probability <= 1.0

    def test_resolve_empty_records(self) -> None:
        """Test resolve with no records returns empty list."""
        linker = IdentityLinker()

        identities = linker.resolve()

        assert identities == []

    def test_resolve_with_custom_threshold(self) -> None:
        """Test resolve respects match_threshold parameter."""
        linker = IdentityLinker()

        linker.add_records(
            [
                {"id": "1", "email": "jane@example.com", "first_name": "Jane"},
                {"id": "2", "email": "different@example.com", "first_name": "Jane"},
            ],
            source="test",
        )

        # Lower threshold might link them by name
        identities_low = linker.resolve(match_threshold=0.3)

        # Higher threshold should be more strict
        identities_high = linker.resolve(match_threshold=0.9)

        # Both should return results (specific matching depends on Splink)
        assert isinstance(identities_low, list)
        assert isinstance(identities_high, list)
