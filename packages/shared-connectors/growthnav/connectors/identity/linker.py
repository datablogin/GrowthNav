"""Identity linker using Splink for probabilistic record matching.

This module provides identity resolution capabilities using both probabilistic
matching (via Splink) and deterministic exact matching. It can link customer
records across multiple source systems based on email, phone, name, and other
identity fields.

Example:
    >>> with IdentityLinker() as linker:
    ...     linker.add_records(shopify_records, source="shopify")
    ...     linker.add_records(square_records, source="square")
    ...     identities = linker.resolve(match_threshold=0.7)
    ...     print(f"Found {len(identities)} unique identities")
"""

from __future__ import annotations

import contextlib
import logging
import re
import uuid
from collections import defaultdict
from typing import Any

from growthnav.connectors.identity.fragments import (
    IdentityFragment,
    IdentityType,
    ResolvedIdentity,
)

logger = logging.getLogger(__name__)

# Email validation pattern
# Matches: local-part@domain.tld
# - Local part: alphanumeric, dots, underscores, percent, plus, hyphen
# - Domain: alphanumeric and dots, must end with TLD of at least 2 chars
EMAIL_PATTERN = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")


class IdentityLinker:
    """Link customer identities across multiple source systems.

    Uses probabilistic matching (Splink) or deterministic exact matching to
    resolve customer identities from different data sources. Supports email,
    phone, name, hashed credit card, and loyalty ID matching.

    Note:
        This class is NOT thread-safe. Create separate instances for
        concurrent resolution tasks. All records are stored in memory,
        so expect approximately 1-2 KB per record. For 1M records,
        expect 1-2 GB RAM usage during resolution.

    Example:
        >>> # Probabilistic matching with context manager (recommended)
        >>> with IdentityLinker() as linker:
        ...     linker.add_records(
        ...         [{"id": "1", "email": "john@example.com", "phone": "555-1234"}],
        ...         source="shopify"
        ...     )
        ...     linker.add_records(
        ...         [{"id": "2", "email": "john@exmple.com", "phone": "5551234"}],
        ...         source="square"
        ...     )
        ...     identities = linker.resolve(match_threshold=0.7)
        >>> # Resources are automatically cleaned up

        >>> # Deterministic matching
        >>> with IdentityLinker() as linker:
        ...     linker.add_records(records, source="shopify")
        ...     identities = linker.resolve_deterministic()
        >>> # Only exact matches on email/phone/etc will be linked
    """

    # Comparison settings for Splink
    COMPARISON_SETTINGS = {
        "email": {"method": "levenshtein", "threshold": 2},
        "phone": {"method": "exact"},
        "name": {"method": "jaro_winkler", "threshold": 0.88},
    }

    def __init__(self) -> None:
        """Initialize the identity linker."""
        self._records: list[dict[str, Any]] = []
        self._linker = None
        self._model_trained = False
        self._closed = False

    def __enter__(self) -> IdentityLinker:
        """Enter context manager.

        Returns:
            Self for use in with statement.
        """
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit context manager and clean up resources.

        Args:
            exc_type: Exception type if an exception was raised.
            exc_val: Exception value if an exception was raised.
            exc_tb: Exception traceback if an exception was raised.
        """
        self.close()

    def __del__(self) -> None:
        """Clean up when object is garbage collected."""
        with contextlib.suppress(Exception):
            self.close()


    def close(self) -> None:
        """Clean up Splink resources.

        Closes the DuckDB connection if Splink was used and clears internal state.
        Safe to call multiple times - will only clean up once.
        """
        if self._closed:
            return

        if self._linker is not None:
            # Close DuckDB connection if available
            if hasattr(self._linker, "db_api"):
                try:
                    self._linker.db_api.close()
                except Exception as e:
                    logger.warning(f"Error closing Splink db_api: {e}")
            self._linker = None

        # Clear records to free memory
        self._records.clear()
        self._model_trained = False
        self._closed = True

    def add_records(
        self,
        records: list[dict],
        source: str,
        id_column: str = "id",
    ) -> None:
        """Add records from a data source for identity resolution.

        Normalizes and extracts identity fields from each record. Records are
        stored internally until resolve() or resolve_deterministic() is called.

        Args:
            records: List of raw records from a source system.
            source: Name of the source system (e.g., "shopify", "square").
            id_column: Name of the ID column in the records. Defaults to "id".

        Example:
            >>> linker = IdentityLinker()
            >>> shopify_records = [
            ...     {"customer_id": "1", "email": "jane@example.com"},
            ...     {"customer_id": "2", "email": "john@example.com"},
            ... ]
            >>> linker.add_records(shopify_records, source="shopify", id_column="customer_id")
        """
        for record in records:
            # Extract source ID
            source_id = record.get(id_column, "")
            if not source_id:
                logger.warning(f"Record missing {id_column} from {source}, skipping")
                continue

            # Create normalized record
            normalized = {
                "source_system": source,
                "source_id": str(source_id),
                "email": self._normalize_email(record),
                "phone": self._normalize_phone(record),
                "first_name": self._normalize_name(
                    record.get("first_name") or record.get("fname") or ""
                ),
                "last_name": self._normalize_name(
                    record.get("last_name") or record.get("lname") or ""
                ),
                "hashed_cc": record.get("hashed_cc") or record.get("cc_hash") or "",
                "loyalty_id": record.get("loyalty_id") or record.get("member_id") or "",
            }

            # Add unique ID for Splink processing
            normalized["unique_id"] = f"{source}_{source_id}"

            self._records.append(normalized)

        logger.info(f"Added {len(records)} records from {source}")

    def _normalize_email(self, record: dict) -> str:
        """Normalize email address to lowercase and strip whitespace.

        Validates email format using a regex pattern that checks for:
        - Valid local part (before @): alphanumeric, dots, underscores, percent, plus, hyphen
        - Valid domain (after @): alphanumeric and dots, with TLD of at least 2 characters

        Args:
            record: Raw record dictionary.

        Returns:
            Normalized email address or empty string if invalid.

        Examples:
            >>> linker = IdentityLinker()
            >>> linker._normalize_email({"email": "user@example.com"})
            'user@example.com'
            >>> linker._normalize_email({"email": "@@@"})
            ''
            >>> linker._normalize_email({"email": "a@b"})
            ''
        """
        email = record.get("email") or record.get("email_address") or ""
        if isinstance(email, str):
            email = email.lower().strip()
            # Validate using regex pattern
            if EMAIL_PATTERN.match(email):
                return email
        return ""

    def _normalize_phone(self, record: dict) -> str:
        """Normalize phone number to last 10 digits (US format).

        Extracts only digits and keeps the last 10 for US phone numbers.
        Phone numbers with fewer than 10 digits are rejected as invalid.

        Note:
            This implementation is US-centric, expecting 10-digit phone numbers.
            International phone numbers (e.g., UK +44, AU +61) may not normalize
            correctly. For international support, consider using the phonenumbers
            library in a future enhancement.

        Args:
            record: Raw record dictionary.

        Returns:
            Normalized 10-digit phone number or empty string if invalid.
        """
        phone = record.get("phone") or record.get("phone_number") or ""
        if isinstance(phone, str):
            # Extract digits only
            digits = re.sub(r"\D", "", phone)
            # Only return if we have at least 10 digits (US format)
            if len(digits) >= 10:
                return digits[-10:]
            # Reject short phone numbers as invalid
        return ""

    def _normalize_name(self, name: str | None) -> str:
        """Normalize name to lowercase and strip whitespace.

        Args:
            name: Raw name string.

        Returns:
            Normalized name or empty string.
        """
        if isinstance(name, str):
            return name.lower().strip()
        return ""

    def resolve(self, match_threshold: float = 0.7) -> list[ResolvedIdentity]:
        """Resolve identities using probabilistic matching with Splink.

        Uses fuzzy matching algorithms to link records that may have typos,
        variations, or incomplete information. Trains a probabilistic model
        to estimate match likelihood.

        Args:
            match_threshold: Minimum probability threshold for considering records
                as matching (0.0 to 1.0). Higher values are more conservative.
                Defaults to 0.7.

        Returns:
            List of resolved identities with linked fragments.

        Raises:
            ImportError: If Splink is not installed. Install with
                `pip install growthnav-connectors[identity]`.

        Example:
            >>> linker = IdentityLinker()
            >>> linker.add_records(records, source="shopify")
            >>> identities = linker.resolve(match_threshold=0.8)
            >>> for identity in identities:
            ...     print(f"Global ID: {identity.global_id}")
            ...     print(f"Fragments: {len(identity.fragments)}")
        """
        try:
            from splink import DuckDBAPI, block_on
            from splink.comparison_library import (
                EmailComparison,
                ExactMatch,
                JaroWinklerAtThresholds,
            )
            from splink.settings import SettingsCreator
        except ImportError as e:
            raise ImportError(
                "Splink is required for probabilistic identity resolution. "
                "Install with: pip install growthnav-connectors[identity]"
            ) from e

        if not self._records:
            logger.warning("No records to resolve")
            return []

        logger.info(f"Resolving {len(self._records)} records with Splink")

        # Initialize Splink with DuckDB backend
        db_api = DuckDBAPI()

        # Create settings for deduplication
        settings = SettingsCreator(
            link_type="dedupe_only",
            blocking_rules_to_generate_predictions=[
                block_on("email"),
                block_on("phone"),
                block_on("hashed_cc"),
                block_on("loyalty_id"),
                block_on("first_name", "last_name"),
            ],
            comparisons=[
                EmailComparison("email"),
                ExactMatch("phone"),
                ExactMatch("hashed_cc"),
                ExactMatch("loyalty_id"),
                JaroWinklerAtThresholds("first_name", [0.88, 0.94]),
                JaroWinklerAtThresholds("last_name", [0.88, 0.94]),
            ],
            unique_id_column_name="unique_id",
        )

        # Create linker
        linker = db_api.linker_from_settings_obj(
            settings=settings.generate_settings_dict(),
            input_table_or_tables=self._records,
        )
        self._linker = linker

        # Train the model if not already trained
        if not self._model_trained:
            logger.info("Training Splink model...")

            # Estimate probability that two random records match
            linker.training.estimate_probability_two_random_records_match(
                ["block_on(email)", "block_on(phone)"],
                recall=0.8,
            )

            # Estimate u parameters using random sampling
            linker.training.estimate_u_using_random_sampling(max_pairs=1e6)

            # Estimate m parameters for comparisons
            training_blocking_rules = [
                "block_on(email)",
                "block_on(phone)",
                "block_on(hashed_cc)",
                "block_on(loyalty_id)",
            ]

            for blocking_rule in training_blocking_rules:
                try:
                    linker.training.estimate_parameters_using_expectation_maximisation(
                        blocking_rule
                    )
                except Exception as e:
                    logger.warning(
                        f"Could not estimate parameters for {blocking_rule}: {e}"
                    )

            self._model_trained = True
            logger.info("Model training completed")

        # Get predictions
        logger.info("Generating predictions...")
        predictions = linker.inference.predict(threshold_match_probability=match_threshold)

        # Cluster records
        logger.info("Clustering records...")
        clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(
            predictions, threshold_match_probability=match_threshold
        )

        # Convert to pandas DataFrame for processing
        cluster_df = clusters.as_pandas_dataframe()

        # Build resolved identities
        identities = self._build_identities(cluster_df)

        logger.info(f"Resolved to {len(identities)} unique identities")
        return identities

    def _build_identities(self, cluster_df: Any) -> list[ResolvedIdentity]:
        """Build ResolvedIdentity objects from Splink cluster results.

        Args:
            cluster_df: DataFrame from Splink clustering with cluster_id column.

        Returns:
            List of ResolvedIdentity objects with unique fragments.

        Note:
            Fragment deduplication uses "first-wins" strategy: when the same
            identity value (e.g., email) appears in multiple source systems,
            only the first occurrence is kept. The source_system field will
            reflect whichever source was processed first. Records are processed
            in the order they appear in the cluster DataFrame.
        """
        # Group records by cluster ID
        clusters = defaultdict(list)
        for _, row in cluster_df.iterrows():
            cluster_id = row["cluster_id"]
            clusters[cluster_id].append(row.to_dict())

        identities = []
        for _cluster_id, records in clusters.items():
            # Generate global ID for this identity
            global_id = str(uuid.uuid4())

            # Collect all identity fragments - deduplicate by type+value only
            fragments: list[IdentityFragment] = []
            seen_values: set[tuple[IdentityType, str]] = set()

            for record in records:
                # Create fragments for each identity type
                source = record.get("source_system", "")

                # Email fragment
                email = record.get("email", "")
                if email:
                    key = (IdentityType.EMAIL, email)
                    if key not in seen_values:
                        fragments.append(
                            IdentityFragment(
                                fragment_type=IdentityType.EMAIL,
                                fragment_value=email,
                                source_system=source,
                            )
                        )
                        seen_values.add(key)

                # Phone fragment
                phone = record.get("phone", "")
                if phone:
                    key = (IdentityType.PHONE, phone)
                    if key not in seen_values:
                        fragments.append(
                            IdentityFragment(
                                fragment_type=IdentityType.PHONE,
                                fragment_value=phone,
                                source_system=source,
                            )
                        )
                        seen_values.add(key)

                # Name fragment (combine first and last)
                first_name = record.get("first_name", "")
                last_name = record.get("last_name", "")
                if first_name or last_name:
                    name = f"{first_name} {last_name}".strip()
                    if name:  # Only add if not empty after strip
                        key = (IdentityType.FULL_NAME, name)
                        if key not in seen_values:
                            fragments.append(
                                IdentityFragment(
                                    fragment_type=IdentityType.FULL_NAME,
                                    fragment_value=name,
                                    source_system=source,
                                )
                            )
                            seen_values.add(key)

                # Hashed CC fragment
                hashed_cc = record.get("hashed_cc", "")
                if hashed_cc:
                    key = (IdentityType.HASHED_CC, str(hashed_cc))
                    if key not in seen_values:
                        fragments.append(
                            IdentityFragment(
                                fragment_type=IdentityType.HASHED_CC,
                                fragment_value=str(hashed_cc),
                                source_system=source,
                            )
                        )
                        seen_values.add(key)

                # Loyalty ID fragment
                loyalty_id = record.get("loyalty_id", "")
                if loyalty_id:
                    key = (IdentityType.LOYALTY_ID, str(loyalty_id))
                    if key not in seen_values:
                        fragments.append(
                            IdentityFragment(
                                fragment_type=IdentityType.LOYALTY_ID,
                                fragment_value=str(loyalty_id),
                                source_system=source,
                            )
                        )
                        seen_values.add(key)

            # Create resolved identity
            # Use average of match probabilities from cluster if available
            match_probs = [
                float(r.get("match_probability", 0.9))
                for r in records
                if r.get("match_probability") is not None
            ]
            avg_probability = sum(match_probs) / len(match_probs) if match_probs else 0.9

            identity = ResolvedIdentity(
                global_id=global_id,
                fragments=fragments,
                match_probability=avg_probability,
            )

            identities.append(identity)

        return identities

    def resolve_deterministic(self) -> list[ResolvedIdentity]:
        """Resolve identities using exact matching only.

        Faster alternative to probabilistic matching when only exact matches
        are needed. Uses union-find algorithm for transitive linking.

        Records are linked if they share any exact match on:
        - Email
        - Phone number
        - Hashed credit card
        - Loyalty ID

        Returns:
            List of resolved identities with match_probability=1.0.

        Example:
            >>> linker = IdentityLinker()
            >>> linker.add_records(records, source="shopify")
            >>> identities = linker.resolve_deterministic()
            >>> # Only exact matches will be linked
        """
        if not self._records:
            logger.warning("No records to resolve")
            return []

        logger.info(f"Resolving {len(self._records)} records deterministically")

        # Union-Find data structure for transitive linking
        parent: dict[int, int] = {}

        def find(x: int) -> int:
            """Find root of set containing x with path compression."""
            if parent[x] != x:
                parent[x] = find(parent[x])
            return parent[x]

        def union(x: int, y: int) -> None:
            """Merge sets containing x and y."""
            root_x = find(x)
            root_y = find(y)
            if root_x != root_y:
                parent[root_x] = root_y

        # Initialize each record as its own set
        for i in range(len(self._records)):
            parent[i] = i

        # Create maps for exact matching
        email_map: dict[str, list[int]] = defaultdict(list)
        phone_map: dict[str, list[int]] = defaultdict(list)
        hashed_cc_map: dict[str, list[int]] = defaultdict(list)
        loyalty_map: dict[str, list[int]] = defaultdict(list)

        # Populate maps
        for i, record in enumerate(self._records):
            if record.get("email"):
                email_map[record["email"]].append(i)
            if record.get("phone"):
                phone_map[record["phone"]].append(i)
            if record.get("hashed_cc"):
                hashed_cc_map[record["hashed_cc"]].append(i)
            if record.get("loyalty_id"):
                loyalty_map[record["loyalty_id"]].append(i)

        # Link records with exact matches
        for indices in email_map.values():
            for i in range(1, len(indices)):
                union(indices[0], indices[i])

        for indices in phone_map.values():
            for i in range(1, len(indices)):
                union(indices[0], indices[i])

        for indices in hashed_cc_map.values():
            for i in range(1, len(indices)):
                union(indices[0], indices[i])

        for indices in loyalty_map.values():
            for i in range(1, len(indices)):
                union(indices[0], indices[i])

        # Group records by cluster
        clusters: dict[int, list[int]] = defaultdict(list)
        for i in range(len(self._records)):
            root = find(i)
            clusters[root].append(i)

        # Build resolved identities
        identities = []
        for record_indices in clusters.values():
            # Generate global ID
            global_id = str(uuid.uuid4())

            # Collect fragments - deduplicate by type+value only
            fragments: list[IdentityFragment] = []
            seen_values: set[tuple[IdentityType, str]] = set()

            for idx in record_indices:
                record = self._records[idx]
                source = record["source_system"]

                # Email fragment
                if record.get("email"):
                    key = (IdentityType.EMAIL, record["email"])
                    if key not in seen_values:
                        fragments.append(
                            IdentityFragment(
                                fragment_type=IdentityType.EMAIL,
                                fragment_value=record["email"],
                                source_system=source,
                            )
                        )
                        seen_values.add(key)

                # Phone fragment
                if record.get("phone"):
                    key = (IdentityType.PHONE, record["phone"])
                    if key not in seen_values:
                        fragments.append(
                            IdentityFragment(
                                fragment_type=IdentityType.PHONE,
                                fragment_value=record["phone"],
                                source_system=source,
                            )
                        )
                        seen_values.add(key)

                # Name fragment
                first_name = record.get("first_name", "")
                last_name = record.get("last_name", "")
                if first_name or last_name:
                    name = f"{first_name} {last_name}".strip()
                    if name:  # Only add if not empty after strip
                        key = (IdentityType.FULL_NAME, name)
                        if key not in seen_values:
                            fragments.append(
                                IdentityFragment(
                                    fragment_type=IdentityType.FULL_NAME,
                                    fragment_value=name,
                                    source_system=source,
                                )
                            )
                            seen_values.add(key)

                # Hashed CC fragment
                if record.get("hashed_cc"):
                    key = (IdentityType.HASHED_CC, str(record["hashed_cc"]))
                    if key not in seen_values:
                        fragments.append(
                            IdentityFragment(
                                fragment_type=IdentityType.HASHED_CC,
                                fragment_value=str(record["hashed_cc"]),
                                source_system=source,
                            )
                        )
                        seen_values.add(key)

                # Loyalty ID fragment
                if record.get("loyalty_id"):
                    key = (IdentityType.LOYALTY_ID, str(record["loyalty_id"]))
                    if key not in seen_values:
                        fragments.append(
                            IdentityFragment(
                                fragment_type=IdentityType.LOYALTY_ID,
                                fragment_value=str(record["loyalty_id"]),
                                source_system=source,
                            )
                        )
                        seen_values.add(key)

            # Create resolved identity with exact match probability
            identity = ResolvedIdentity(
                global_id=global_id,
                fragments=fragments,
                match_probability=1.0,  # Exact matches only
            )

            identities.append(identity)

        logger.info(f"Resolved to {len(identities)} unique identities")
        return identities
