"""Identity resolution services.

This module provides probabilistic identity resolution using Splink
to link customer records across multiple identity fragments (email,
phone, hashed CC, loyalty ID).

Example:
    from growthnav.connectors.identity import IdentityLinker, IdentityType

    linker = IdentityLinker()
    linker.add_records(pos_data, source="toast")
    linker.add_records(olo_data, source="olo")

    # Probabilistic matching (requires splink)
    identities = linker.resolve()

    # Or use faster deterministic matching
    identities = linker.resolve_deterministic()

    for identity in identities:
        print(f"Global ID: {identity.global_id}")
        print(f"  Emails: {identity.emails}")
        print(f"  Match confidence: {identity.match_probability:.0%}")
"""

from growthnav.connectors.identity.fragments import (
    IdentityFragment,
    IdentityType,
    ResolvedIdentity,
)
from growthnav.connectors.identity.linker import IdentityLinker

__all__ = [
    "IdentityFragment",
    "IdentityLinker",
    "IdentityType",
    "ResolvedIdentity",
]
