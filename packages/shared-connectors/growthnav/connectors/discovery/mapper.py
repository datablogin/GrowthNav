"""LLM-assisted schema mapping for automatic field detection.

This module provides intelligent schema mapping using Claude to suggest how source
system fields should map to the target Conversion schema. It analyzes field names,
data types, sample values, and patterns to make recommendations with confidence scores.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from typing import Any

from growthnav.connectors.discovery.profiler import ColumnProfile, ColumnProfiler

logger = logging.getLogger(__name__)

# Default model for LLM schema mapping
DEFAULT_MODEL = "claude-sonnet-4-20250514"


@dataclass
class MappingSuggestion:
    """A suggested mapping from source field to target Conversion field.

    Attributes:
        source_field: Name of the field in the source system.
        target_field: Name of the field in Conversion schema, or None if no mapping found.
        confidence: Confidence score from 0.0 to 1.0.
        reason: Human-readable explanation of why this mapping was suggested.
        sample_values: Sample values from the source field to illustrate the data.
    """

    source_field: str
    target_field: str | None
    confidence: float
    reason: str
    sample_values: list[Any]

    def __post_init__(self) -> None:
        """Validate confidence is in valid range."""
        if not 0.0 <= self.confidence <= 1.0:
            raise ValueError(f"Confidence must be between 0.0 and 1.0, got {self.confidence}")


class LLMSchemaMapper:
    """Uses Claude to suggest schema mappings from source to Conversion schema.

    This mapper analyzes source schema profiles and uses Claude's intelligence to
    suggest how fields should map to the standardized Conversion schema. It considers
    field names, data types, patterns, and sample values.

    Example:
        mapper = LLMSchemaMapper()
        suggestions = await mapper.suggest_mappings(
            source_profiles={"order_id": profile1, "total": profile2},
            sample_rows=[{"order_id": "123", "total": 45.99}],
            context="Snowflake POS transaction data"
        )
    """

    # Target Conversion schema with field descriptions for LLM context
    TARGET_SCHEMA = {
        "transaction_id": "Unique identifier for the transaction/purchase (REQUIRED for CLV)",
        "user_id": "Unique identifier for the customer/user",
        "timestamp": "Date and time of the conversion event (REQUIRED for CLV)",
        "value": "Monetary value of the conversion (REQUIRED for CLV)",
        "currency": "Three-letter currency code (e.g., USD, EUR)",
        "quantity": "Number of items/units in the conversion",
        "product_id": "Unique identifier for the product/service",
        "product_name": "Human-readable product/service name",
        "product_category": "Category or type of product/service",
        "location_id": "Unique identifier for the business location",
        "location_name": "Human-readable location name",
        "gclid": "Google Click ID for attribution",
        "fbclid": "Facebook Click ID for attribution",
        "utm_source": "UTM source parameter for attribution",
        "utm_medium": "UTM medium parameter for attribution",
        "utm_campaign": "UTM campaign parameter for attribution",
    }

    def __init__(
        self,
        anthropic_client: Any | None = None,
        model: str = DEFAULT_MODEL,
    ) -> None:
        """Initialize the schema mapper.

        Args:
            anthropic_client: Optional pre-configured Anthropic client.
                If None, will be lazy-initialized when needed.
            model: Model to use for LLM requests. Defaults to DEFAULT_MODEL.
        """
        self._client = anthropic_client
        self._model = model

    @property
    def client(self) -> Any:
        """Lazy-initialize and return the Anthropic client.

        Returns:
            Configured Anthropic client instance.

        Raises:
            ImportError: If anthropic package is not installed.
            ValueError: If ANTHROPIC_API_KEY environment variable is not set.
        """
        if self._client is None:
            try:
                from anthropic import Anthropic

                api_key = os.getenv("ANTHROPIC_API_KEY")
                if not api_key:
                    raise ValueError(
                        "ANTHROPIC_API_KEY environment variable not set. "
                        "Required for LLM schema mapping."
                    )
                self._client = Anthropic(api_key=api_key)
                logger.debug("Initialized Anthropic client for schema mapping")
            except ImportError as e:
                raise ImportError(
                    "Anthropic client required for LLM schema mapping. "
                    "Install with: uv add --optional llm anthropic"
                ) from e
        return self._client

    async def suggest_mappings(
        self,
        source_profiles: dict[str, ColumnProfile],
        sample_rows: list[dict[str, Any]],
        context: str | None = None,
    ) -> list[MappingSuggestion]:
        """Suggest mappings from source schema to Conversion schema using Claude.

        Args:
            source_profiles: Dictionary mapping source field names to their profiles.
            sample_rows: Sample data rows from the source system (max 5 recommended).
            context: Optional context about the data source (e.g., "Snowflake POS data").

        Returns:
            List of mapping suggestions with confidence scores.

        Raises:
            ImportError: If anthropic package is not installed.
        """
        logger.info(
            f"Requesting LLM schema mapping for {len(source_profiles)} source fields"
        )

        # Build the prompt with schema information
        prompt = self._build_prompt(source_profiles, sample_rows, context)

        # Call Claude
        response = self.client.messages.create(
            model=self._model,
            max_tokens=4096,
            messages=[{"role": "user", "content": prompt}],
        )

        # Parse the response
        response_text = response.content[0].text
        suggestions = self._parse_response(response_text, source_profiles)

        logger.info(f"LLM suggested {len(suggestions)} field mappings")
        return suggestions

    def _build_prompt(
        self,
        profiles: dict[str, ColumnProfile],
        sample_rows: list[dict[str, Any]],
        context: str | None,
    ) -> str:
        """Build the prompt for Claude with schema analysis.

        Args:
            profiles: Source field profiles.
            sample_rows: Sample data rows.
            context: Optional context string.

        Returns:
            Formatted prompt string.
        """
        # Build source schema section
        source_schema = []
        for field_name, profile in profiles.items():
            field_info = {
                "name": field_name,
                "type": profile.inferred_type,
                "null_percentage": round(profile.null_percentage, 2),
                "unique_values": profile.unique_count,
                "sample_values": profile.sample_values[:5],
            }
            if profile.detected_patterns:
                field_info["patterns"] = list(profile.detected_patterns)[:3]
            source_schema.append(field_info)

        # Build target schema section
        target_schema = [
            {"name": name, "description": desc}
            for name, desc in self.TARGET_SCHEMA.items()
        ]

        # Limit sample rows to prevent token overflow
        limited_samples = sample_rows[:5]

        prompt = f"""You are a data integration expert. Analyze the source schema and suggest how fields should map to the target Conversion schema.

Context: {context or "Unknown data source"}

SOURCE SCHEMA:
{json.dumps(source_schema, indent=2)}

TARGET CONVERSION SCHEMA:
{json.dumps(target_schema, indent=2)}

SAMPLE DATA:
{json.dumps(limited_samples, indent=2, default=str)}

IMPORTANT NOTES:
- transaction_id, timestamp, and value are REQUIRED fields for Customer Lifetime Value (CLV) analysis
- Look for common field name patterns (e.g., "order_id" -> "transaction_id", "total" -> "value")
- Consider data types and patterns when suggesting mappings
- If a source field doesn't clearly map to any target field, set target_field to null
- Confidence should be:
  * 0.9-1.0: Exact or near-exact name match with correct data type
  * 0.7-0.9: Strong semantic match (e.g., "total_amount" -> "value")
  * 0.5-0.7: Reasonable match based on type/patterns but ambiguous name
  * 0.3-0.5: Weak match, might be correct but uncertain
  * 0.0-0.3: Very uncertain or no clear match

Respond with a JSON array of mapping suggestions. Each suggestion must have:
- source_field: name of the source field
- target_field: name of target Conversion field (or null if no match)
- confidence: float between 0.0 and 1.0
- reason: brief explanation of the mapping decision

Example response format:
[
  {{"source_field": "order_id", "target_field": "transaction_id", "confidence": 0.95, "reason": "Field name strongly suggests transaction identifier"}},
  {{"source_field": "total", "target_field": "value", "confidence": 0.85, "reason": "Numeric field likely represents transaction value"}},
  {{"source_field": "internal_code", "target_field": null, "confidence": 0.1, "reason": "No clear mapping to Conversion schema"}}
]

Respond with ONLY the JSON array, no other text."""

        return prompt

    def _parse_response(
        self,
        response_text: str,
        profiles: dict[str, ColumnProfile],
    ) -> list[MappingSuggestion]:
        """Parse Claude's JSON response into MappingSuggestion objects.

        Args:
            response_text: Raw text response from Claude.
            profiles: Source field profiles for extracting sample values.

        Returns:
            List of MappingSuggestion objects.

        Raises:
            ValueError: If response cannot be parsed as valid JSON.
        """
        try:
            # Extract JSON from response (handle markdown code blocks)
            json_text = response_text.strip()
            if json_text.startswith("```"):
                # Remove markdown code fence - handle both opening and closing
                lines = json_text.split("\n")
                # Remove first line (opening fence like ```json or ```)
                lines = lines[1:]
                # Remove last line if it's a closing fence
                if lines and lines[-1].strip().startswith("```"):
                    lines = lines[:-1]
                json_text = "\n".join(lines)

            suggestions_data = json.loads(json_text)

            suggestions = []
            for item in suggestions_data:
                source_field = item["source_field"]
                profile = profiles.get(source_field)

                # Clamp confidence to valid range [0.0, 1.0]
                raw_confidence = float(item["confidence"])
                confidence = max(0.0, min(1.0, raw_confidence))
                if confidence != raw_confidence:
                    logger.warning(
                        f"Clamped confidence for {source_field} from {raw_confidence} to {confidence}"
                    )

                suggestion = MappingSuggestion(
                    source_field=source_field,
                    target_field=item.get("target_field"),
                    confidence=confidence,
                    reason=item["reason"],
                    sample_values=profile.sample_values if profile else [],
                )
                suggestions.append(suggestion)

            return suggestions

        except (json.JSONDecodeError, KeyError, ValueError) as e:
            logger.error(f"Failed to parse LLM response: {e}")
            logger.debug(f"Response text: {response_text}")
            raise ValueError(f"Invalid LLM response format: {e}") from e


class SchemaDiscovery:
    """Complete schema discovery pipeline with profiling and LLM mapping.

    Combines column profiling with LLM-assisted mapping to provide a comprehensive
    analysis of how source data should map to the Conversion schema.

    Example:
        discovery = SchemaDiscovery()
        result = await discovery.analyze(
            data=[{"order_id": "123", "total": 45.99, "date": "2025-01-15"}],
            context="Snowflake POS transactions"
        )
        print(f"High confidence mappings: {result['field_map']}")
        print(f"Confidence summary: {result['confidence_summary']}")
    """

    def __init__(self, anthropic_client: Any | None = None) -> None:
        """Initialize schema discovery.

        Args:
            anthropic_client: Optional pre-configured Anthropic client.
        """
        self._profiler = ColumnProfiler()
        self._mapper = LLMSchemaMapper(anthropic_client)

    async def analyze(
        self,
        data: list[dict[str, Any]],
        context: str | None = None,
    ) -> dict[str, Any]:
        """Analyze source data and suggest Conversion schema mappings.

        Args:
            data: List of data records from the source system.
            context: Optional context about the data source.

        Returns:
            Dictionary containing:
                - profiles: dict[str, ColumnProfile] - Field profiles
                - suggestions: list[MappingSuggestion] - All mapping suggestions
                - field_map: dict[str, str] - High-confidence mappings (>= 0.7)
                - confidence_summary: dict with high/medium/low counts
        """
        if not data:
            return {
                "profiles": {},
                "suggestions": [],
                "field_map": {},
                "confidence_summary": {"high": 0, "medium": 0, "low": 0, "unmapped": 0},
            }

        logger.info(f"Analyzing schema for {len(data)} records")

        # Profile the source columns
        profiles = self._profiler.profile(data)
        logger.debug(f"Profiled {len(profiles)} columns")

        # Get LLM mapping suggestions
        suggestions = await self._mapper.suggest_mappings(profiles, data, context)

        # Build high-confidence field map (>= 0.7)
        field_map = {
            s.source_field: s.target_field
            for s in suggestions
            if s.target_field and s.confidence >= 0.7
        }

        # Calculate confidence summary
        high_confidence = sum(1 for s in suggestions if s.confidence >= 0.7 and s.target_field)
        medium_confidence = sum(
            1 for s in suggestions if 0.5 <= s.confidence < 0.7 and s.target_field
        )
        low_confidence = sum(1 for s in suggestions if 0.3 <= s.confidence < 0.5 and s.target_field)
        unmapped = sum(1 for s in suggestions if not s.target_field or s.confidence < 0.3)

        confidence_summary = {
            "high": high_confidence,
            "medium": medium_confidence,
            "low": low_confidence,
            "unmapped": unmapped,
        }

        logger.info(
            f"Schema analysis complete: {high_confidence} high-confidence mappings, "
            f"{medium_confidence} medium, {low_confidence} low, {unmapped} unmapped"
        )

        return {
            "profiles": profiles,
            "suggestions": suggestions,
            "field_map": field_map,
            "confidence_summary": confidence_summary,
        }
