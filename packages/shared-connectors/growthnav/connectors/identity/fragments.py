"""Identity fragment models for customer identity resolution.

This module provides the core data structures for representing customer identity
information from various sources. Identity fragments are combined to create a
unified view of customers across multiple touchpoints.

Examples:
    Creating identity fragments:
        >>> email_frag = IdentityFragment(
        ...     fragment_type=IdentityType.EMAIL,
        ...     fragment_value="john@example.com",
        ...     source_system="shopify"
        ... )
        >>> phone_frag = IdentityFragment(
        ...     fragment_type=IdentityType.PHONE,
        ...     fragment_value="555-1234",
        ...     source_system="square",
        ...     confidence=0.9
        ... )

    Creating a resolved identity:
        >>> identity = ResolvedIdentity(
        ...     global_id="cust_abc123",
        ...     fragments=[email_frag, phone_frag],
        ...     match_probability=0.95
        ... )
        >>> identity.emails
        ['john@example.com']
        >>> identity.has_fragment_type(IdentityType.EMAIL)
        True
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class IdentityType(str, Enum):
    """Types of identity fragments that can be collected from source systems.

    Each type represents a different way to identify or match customers across
    systems. Some types (like EMAIL, PHONE) are strong identifiers, while others
    (like COOKIE_ID, DEVICE_ID) are weaker and may require additional signals.
    """

    EMAIL = "email"
    """Email address - strong identifier, normalized to lowercase"""

    PHONE = "phone"
    """Phone number - strong identifier, normalized to digits only"""

    HASHED_CC = "hashed_cc"
    """Hashed credit card number - strong identifier, privacy-preserving"""

    LOYALTY_ID = "loyalty_id"
    """Loyalty program ID - strong identifier within a business"""

    DEVICE_ID = "device_id"
    """Device identifier (IDFA, GAID, etc.) - medium strength, can change"""

    COOKIE_ID = "cookie_id"
    """Browser cookie ID - weak identifier, frequently cleared"""

    CUSTOMER_ID = "customer_id"
    """Source system's customer ID - strong within that system"""

    NAME_ZIP = "name_zip"
    """Name + ZIP code composite - medium strength, useful for matching"""


@dataclass
class IdentityFragment:
    """A single piece of customer identity information from a source system.

    Identity fragments are the building blocks of customer identity resolution.
    Each fragment represents one way to identify a customer, along with metadata
    about where it came from and how confident we are in its accuracy.

    Fragments are compared using normalized values to ensure case-insensitive
    matching (e.g., "John@Example.com" equals "john@example.com").

    Attributes:
        fragment_type: The type of identity information this represents
        fragment_value: The actual identity value (email, phone, etc.)
        source_system: Optional name of the source system (e.g., "shopify", "square")
        confidence: Confidence in this fragment's accuracy (0.0 to 1.0)
        metadata: Additional context about this fragment

    Examples:
        >>> # High-confidence email from Shopify
        >>> email = IdentityFragment(
        ...     fragment_type=IdentityType.EMAIL,
        ...     fragment_value="customer@example.com",
        ...     source_system="shopify",
        ...     confidence=1.0
        ... )
        >>>
        >>> # Medium-confidence phone from incomplete data
        >>> phone = IdentityFragment(
        ...     fragment_type=IdentityType.PHONE,
        ...     fragment_value="555-1234",
        ...     source_system="square",
        ...     confidence=0.7,
        ...     metadata={"note": "partial number from form"}
        ... )
        >>>
        >>> # Fragments with same type and value are equal
        >>> frag1 = IdentityFragment(IdentityType.EMAIL, "Test@Example.com")
        >>> frag2 = IdentityFragment(IdentityType.EMAIL, "test@example.com")
        >>> frag1 == frag2
        True
    """

    fragment_type: IdentityType
    fragment_value: str
    source_system: str | None = None
    confidence: float = 1.0
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate fragment attributes after initialization."""
        if not 0.0 <= self.confidence <= 1.0:
            raise ValueError(f"Confidence must be between 0.0 and 1.0, got {self.confidence}")

    def _normalize_value(self) -> str:
        """Normalize the fragment value for comparison.

        Returns:
            Normalized value (lowercase, stripped of whitespace)
        """
        return self.fragment_value.lower().strip()

    def __hash__(self) -> int:
        """Hash based on fragment type and normalized value.

        This allows fragments to be used in sets and as dictionary keys,
        ensuring that fragments with the same type and value (ignoring case)
        are treated as duplicates.

        Returns:
            Hash value based on type and normalized value
        """
        return hash((self.fragment_type, self._normalize_value()))

    def __eq__(self, other: object) -> bool:
        """Compare fragments by type and normalized value.

        Two fragments are equal if they have the same type and the same
        normalized value (case-insensitive comparison).

        Args:
            other: Object to compare with

        Returns:
            True if fragments have same type and normalized value

        Examples:
            >>> frag1 = IdentityFragment(IdentityType.EMAIL, "John@Example.COM")
            >>> frag2 = IdentityFragment(IdentityType.EMAIL, "john@example.com")
            >>> frag1 == frag2
            True
            >>> frag3 = IdentityFragment(IdentityType.PHONE, "555-1234")
            >>> frag1 == frag3
            False
        """
        if not isinstance(other, IdentityFragment):
            return NotImplemented
        return (
            self.fragment_type == other.fragment_type
            and self._normalize_value() == other._normalize_value()
        )


@dataclass
class ResolvedIdentity:
    """A unified customer identity resolved from multiple fragments.

    This represents a single customer entity that may be known by multiple
    identifiers across different systems. The global_id is GrowthNav's unified
    identifier for this customer.

    Attributes:
        global_id: GrowthNav's unified customer identifier
        fragments: List of identity fragments that belong to this customer
        match_probability: Overall confidence in this identity resolution (0.0 to 1.0)

    Examples:
        >>> # Create a resolved identity with multiple fragments
        >>> identity = ResolvedIdentity(
        ...     global_id="gn_cust_abc123",
        ...     fragments=[
        ...         IdentityFragment(IdentityType.EMAIL, "john@example.com", "shopify"),
        ...         IdentityFragment(IdentityType.PHONE, "555-1234", "square"),
        ...         IdentityFragment(IdentityType.LOYALTY_ID, "LOYAL123", "toast")
        ...     ],
        ...     match_probability=0.95
        ... )
        >>>
        >>> # Access specific identity types
        >>> identity.emails
        ['john@example.com']
        >>> identity.phones
        ['555-1234']
        >>>
        >>> # Check for specific fragment types
        >>> identity.has_fragment_type(IdentityType.EMAIL)
        True
        >>> identity.has_fragment_type(IdentityType.DEVICE_ID)
        False
    """

    global_id: str
    fragments: list[IdentityFragment] = field(default_factory=list)
    match_probability: float = 1.0

    def __post_init__(self) -> None:
        """Validate identity attributes after initialization."""
        if not 0.0 <= self.match_probability <= 1.0:
            raise ValueError(
                f"Match probability must be between 0.0 and 1.0, got {self.match_probability}"
            )

    @property
    def emails(self) -> list[str]:
        """Get all email addresses associated with this identity.

        Returns:
            List of email addresses (fragment values)

        Examples:
            >>> identity = ResolvedIdentity(
            ...     global_id="cust_123",
            ...     fragments=[
            ...         IdentityFragment(IdentityType.EMAIL, "john@example.com"),
            ...         IdentityFragment(IdentityType.EMAIL, "john.doe@work.com"),
            ...         IdentityFragment(IdentityType.PHONE, "555-1234")
            ...     ]
            ... )
            >>> identity.emails
            ['john@example.com', 'john.doe@work.com']
        """
        return [
            frag.fragment_value
            for frag in self.fragments
            if frag.fragment_type == IdentityType.EMAIL
        ]

    @property
    def phones(self) -> list[str]:
        """Get all phone numbers associated with this identity.

        Returns:
            List of phone numbers (fragment values)

        Examples:
            >>> identity = ResolvedIdentity(
            ...     global_id="cust_123",
            ...     fragments=[
            ...         IdentityFragment(IdentityType.PHONE, "555-1234"),
            ...         IdentityFragment(IdentityType.PHONE, "555-5678"),
            ...         IdentityFragment(IdentityType.EMAIL, "john@example.com")
            ...     ]
            ... )
            >>> identity.phones
            ['555-1234', '555-5678']
        """
        return [
            frag.fragment_value
            for frag in self.fragments
            if frag.fragment_type == IdentityType.PHONE
        ]

    def has_fragment_type(self, fragment_type: IdentityType) -> bool:
        """Check if this identity has at least one fragment of the given type.

        Args:
            fragment_type: The identity type to check for

        Returns:
            True if at least one fragment of this type exists

        Examples:
            >>> identity = ResolvedIdentity(
            ...     global_id="cust_123",
            ...     fragments=[
            ...         IdentityFragment(IdentityType.EMAIL, "john@example.com"),
            ...         IdentityFragment(IdentityType.PHONE, "555-1234")
            ...     ]
            ... )
            >>> identity.has_fragment_type(IdentityType.EMAIL)
            True
            >>> identity.has_fragment_type(IdentityType.DEVICE_ID)
            False
        """
        return any(frag.fragment_type == fragment_type for frag in self.fragments)
