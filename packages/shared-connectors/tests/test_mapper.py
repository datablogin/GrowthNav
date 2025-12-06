"""Tests for growthnav.connectors.discovery.mapper."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from growthnav.connectors.discovery.mapper import (
    LLMSchemaMapper,
    MappingSuggestion,
    SchemaDiscovery,
)
from growthnav.connectors.discovery.profiler import ColumnProfile, ColumnProfiler


class TestMappingSuggestion:
    """Tests for MappingSuggestion dataclass."""

    def test_dataclass_fields_have_correct_defaults(self) -> None:
        """Test MappingSuggestion fields have no default values (all required)."""
        # All fields are required, so creating without them should fail
        with pytest.raises(TypeError):
            MappingSuggestion()  # type: ignore[call-arg]

    def test_dataclass_is_properly_instantiated(self) -> None:
        """Test MappingSuggestion can be instantiated with all fields."""
        suggestion = MappingSuggestion(
            source_field="order_id",
            target_field="transaction_id",
            confidence=0.95,
            reason="Direct ID field match",
            sample_values=["ORD-001", "ORD-002", "ORD-003"],
        )

        assert suggestion.source_field == "order_id"
        assert suggestion.target_field == "transaction_id"
        assert suggestion.confidence == 0.95
        assert suggestion.reason == "Direct ID field match"
        assert suggestion.sample_values == ["ORD-001", "ORD-002", "ORD-003"]

    def test_target_field_can_be_none(self) -> None:
        """Test target_field can be None when no mapping found."""
        suggestion = MappingSuggestion(
            source_field="unknown_field",
            target_field=None,
            confidence=0.1,
            reason="No clear mapping",
            sample_values=["val1", "val2"],
        )

        assert suggestion.target_field is None

    def test_confidence_validation_rejects_above_one(self) -> None:
        """Test __post_init__ rejects confidence > 1.0."""
        with pytest.raises(ValueError, match="Confidence must be between 0.0 and 1.0"):
            MappingSuggestion(
                source_field="test",
                target_field="value",
                confidence=1.5,
                reason="Too confident",
                sample_values=[],
            )

    def test_confidence_validation_rejects_below_zero(self) -> None:
        """Test __post_init__ rejects confidence < 0.0."""
        with pytest.raises(ValueError, match="Confidence must be between 0.0 and 1.0"):
            MappingSuggestion(
                source_field="test",
                target_field="value",
                confidence=-0.1,
                reason="Negative confidence",
                sample_values=[],
            )


class TestLLMSchemaMapper:
    """Tests for LLMSchemaMapper class."""

    def test_init_accepts_anthropic_client_parameter(self) -> None:
        """Test __init__ accepts and stores anthropic_client parameter."""
        mock_client = MagicMock()
        mapper = LLMSchemaMapper(anthropic_client=mock_client)

        assert mapper._client is mock_client

    def test_init_with_no_client(self) -> None:
        """Test __init__ with no client sets _client to None."""
        mapper = LLMSchemaMapper()

        assert mapper._client is None

    def test_client_property_raises_import_error_when_anthropic_not_installed(
        self,
    ) -> None:
        """Test client property raises ImportError when anthropic not installed."""
        mapper = LLMSchemaMapper()

        with (
            patch.dict("sys.modules", {"anthropic": None}),
            pytest.raises(ImportError, match="Anthropic client required"),
        ):
            _ = mapper.client

    def test_client_property_lazy_initializes(self) -> None:
        """Test client property creates AsyncAnthropic client when not provided."""
        mapper = LLMSchemaMapper()

        mock_async_anthropic_class = MagicMock()
        mock_client = MagicMock()
        mock_async_anthropic_class.return_value = mock_client

        # The mapper imports AsyncAnthropic inside the property, so we patch the import
        with (
            patch.dict(
                "sys.modules", {"anthropic": MagicMock(AsyncAnthropic=mock_async_anthropic_class)}
            ),
            patch.dict("os.environ", {"ANTHROPIC_API_KEY": "test-key"}),
        ):
            mapper._client = None  # Reset to trigger lazy init
            client = mapper.client

            assert client is mock_client

    def test_client_property_raises_value_error_when_api_key_missing(self) -> None:
        """Test client property raises ValueError when ANTHROPIC_API_KEY not set."""
        mapper = LLMSchemaMapper()

        mock_anthropic_module = MagicMock()
        mock_anthropic_module.AsyncAnthropic = MagicMock()

        with (
            patch.dict("sys.modules", {"anthropic": mock_anthropic_module}),
            patch.dict("os.environ", {}, clear=True),
            patch("os.getenv", return_value=None),
            pytest.raises(ValueError, match="ANTHROPIC_API_KEY environment variable not set"),
        ):
            _ = mapper.client

    def test_init_accepts_model_parameter(self) -> None:
        """Test __init__ accepts and stores model parameter."""
        custom_model = "claude-3-opus-20240229"
        mapper = LLMSchemaMapper(model=custom_model)

        assert mapper._model == custom_model

    def test_init_uses_default_model(self) -> None:
        """Test __init__ uses DEFAULT_MODEL when no model provided."""
        from growthnav.connectors.discovery.mapper import DEFAULT_MODEL

        mapper = LLMSchemaMapper()

        assert mapper._model == DEFAULT_MODEL

    def test_build_prompt_includes_source_profiles(self) -> None:
        """Test _build_prompt includes source column profiles."""
        mapper = LLMSchemaMapper()
        profiles = {
            "order_id": ColumnProfile(
                name="order_id",
                inferred_type="string",
                total_count=10,
                null_count=0,
                unique_count=10,
                detected_patterns=["uuid"],
                sample_values=["123-abc", "456-def"],
            ),
        }

        prompt = mapper._build_prompt(profiles, [], None)

        assert "order_id" in prompt
        # Check that profile info is in the JSON format
        assert '"name": "order_id"' in prompt
        assert '"type": "string"' in prompt

    def test_build_prompt_includes_target_schema(self) -> None:
        """Test _build_prompt includes target schema fields."""
        mapper = LLMSchemaMapper()
        profiles = {}

        prompt = mapper._build_prompt(profiles, [], None)

        assert "transaction_id" in prompt
        assert "timestamp" in prompt
        assert "value" in prompt
        # The actual header is uppercase
        assert "TARGET CONVERSION SCHEMA" in prompt

    def test_build_prompt_includes_sample_rows_as_json(self) -> None:
        """Test _build_prompt includes sample rows as JSON."""
        mapper = LLMSchemaMapper()
        profiles = {}
        sample_rows = [
            {"order_id": "ORD-001", "amount": 100.0},
            {"order_id": "ORD-002", "amount": 200.0},
        ]

        prompt = mapper._build_prompt(profiles, sample_rows, None)

        assert "ORD-001" in prompt
        assert "100.0" in prompt
        # Sample data is in SAMPLE DATA section, not a code block
        assert "SAMPLE DATA:" in prompt

    def test_build_prompt_includes_context_when_provided(self) -> None:
        """Test _build_prompt includes context when provided."""
        mapper = LLMSchemaMapper()
        profiles = {}
        context = "Toast POS transaction data"

        prompt = mapper._build_prompt(profiles, [], context)

        assert context in prompt

    def test_build_prompt_uses_default_context_when_none(self) -> None:
        """Test _build_prompt uses default context when None provided."""
        mapper = LLMSchemaMapper()
        profiles = {}

        prompt = mapper._build_prompt(profiles, [], None)

        assert "Unknown data source" in prompt

    def test_parse_response_extracts_mappings_from_valid_json_response(self) -> None:
        """Test _parse_response extracts mappings from valid JSON response."""
        mapper = LLMSchemaMapper()
        profiles = {
            "order_id": ColumnProfile(
                name="order_id",
                inferred_type="string",
                total_count=2,
                null_count=0,
                unique_count=2,
                sample_values=["ORD-001", "ORD-002"],
            ),
            "amount": ColumnProfile(
                name="amount",
                inferred_type="number",
                total_count=2,
                null_count=0,
                unique_count=2,
                sample_values=["100.0", "200.0"],
            ),
        }

        response_text = """[
            {
                "source_field": "order_id",
                "target_field": "transaction_id",
                "confidence": 0.95,
                "reason": "Direct ID mapping"
            },
            {
                "source_field": "amount",
                "target_field": "value",
                "confidence": 0.9,
                "reason": "Monetary value field"
            }
        ]"""

        suggestions = mapper._parse_response(response_text, profiles)

        assert len(suggestions) == 2
        assert suggestions[0].source_field == "order_id"
        assert suggestions[0].target_field == "transaction_id"
        assert suggestions[0].confidence == 0.95
        assert suggestions[0].reason == "Direct ID mapping"
        assert suggestions[0].sample_values == ["ORD-001", "ORD-002"]

        assert suggestions[1].source_field == "amount"
        assert suggestions[1].target_field == "value"
        assert suggestions[1].confidence == 0.9

    def test_parse_response_raises_on_invalid_json(self) -> None:
        """Test _parse_response raises ValueError on invalid JSON."""
        mapper = LLMSchemaMapper()
        profiles = {}

        response_text = "This is not JSON at all"

        with pytest.raises(ValueError, match="Invalid LLM response format"):
            mapper._parse_response(response_text, profiles)

    def test_parse_response_handles_missing_fields_gracefully(self) -> None:
        """Test _parse_response raises error on missing required fields."""
        mapper = LLMSchemaMapper()
        profiles = {
            "test_field": ColumnProfile(
                name="test_field",
                inferred_type="string",
                total_count=1,
                null_count=0,
                unique_count=1,
                sample_values=["val1"],
            ),
        }

        # Missing confidence and reason fields - will fail on "confidence" key
        response_text = """[
            {
                "source_field": "test_field",
                "target_field": "transaction_id"
            }
        ]"""

        # The mapper requires confidence and reason keys
        with pytest.raises(ValueError, match="Invalid LLM response format"):
            mapper._parse_response(response_text, profiles)

    def test_parse_response_handles_missing_profile(self) -> None:
        """Test _parse_response handles source field not in profiles."""
        mapper = LLMSchemaMapper()
        profiles = {}

        response_text = """
        [
            {
                "source_field": "unknown_field",
                "target_field": "transaction_id",
                "confidence": 0.5,
                "reason": "Test"
            }
        ]
        """

        suggestions = mapper._parse_response(response_text, profiles)

        assert len(suggestions) == 1
        assert suggestions[0].sample_values == []  # No profile found

    def test_parse_response_handles_markdown_code_fence(self) -> None:
        """Test _parse_response correctly strips markdown code fences."""
        mapper = LLMSchemaMapper()
        profiles = {
            "order_id": ColumnProfile(
                name="order_id",
                inferred_type="string",
                total_count=1,
                null_count=0,
                unique_count=1,
                sample_values=["ORD-001"],
            ),
        }

        # Response with markdown code fence
        response_text = """```json
[
    {
        "source_field": "order_id",
        "target_field": "transaction_id",
        "confidence": 0.95,
        "reason": "Direct ID mapping"
    }
]
```"""

        suggestions = mapper._parse_response(response_text, profiles)

        assert len(suggestions) == 1
        assert suggestions[0].source_field == "order_id"
        assert suggestions[0].target_field == "transaction_id"

    def test_parse_response_handles_markdown_fence_without_closing(self) -> None:
        """Test _parse_response handles markdown fence without proper closing."""
        mapper = LLMSchemaMapper()
        profiles = {}

        # Response with code fence but no closing backticks
        response_text = """```json
[
    {
        "source_field": "test_field",
        "target_field": "value",
        "confidence": 0.8,
        "reason": "Test"
    }
]
"""

        suggestions = mapper._parse_response(response_text, profiles)

        assert len(suggestions) == 1
        assert suggestions[0].source_field == "test_field"

    def test_parse_response_clamps_high_confidence(self) -> None:
        """Test _parse_response clamps confidence values > 1.0."""
        mapper = LLMSchemaMapper()
        profiles = {}

        response_text = """[
            {
                "source_field": "test_field",
                "target_field": "value",
                "confidence": 1.5,
                "reason": "Over-confident LLM"
            }
        ]"""

        suggestions = mapper._parse_response(response_text, profiles)

        assert len(suggestions) == 1
        assert suggestions[0].confidence == 1.0  # Clamped to 1.0

    def test_parse_response_clamps_negative_confidence(self) -> None:
        """Test _parse_response clamps confidence values < 0.0."""
        mapper = LLMSchemaMapper()
        profiles = {}

        response_text = """[
            {
                "source_field": "test_field",
                "target_field": "value",
                "confidence": -0.5,
                "reason": "Negative confidence"
            }
        ]"""

        suggestions = mapper._parse_response(response_text, profiles)

        assert len(suggestions) == 1
        assert suggestions[0].confidence == 0.0  # Clamped to 0.0

    def test_target_schema_has_required_clv_fields(self) -> None:
        """Test TARGET_SCHEMA has required CLV fields."""
        mapper = LLMSchemaMapper()

        # Required fields for CLV analysis
        required_fields = ["transaction_id", "timestamp", "value"]

        for field in required_fields:
            assert field in mapper.TARGET_SCHEMA
            assert isinstance(mapper.TARGET_SCHEMA[field], str)
            assert len(mapper.TARGET_SCHEMA[field]) > 0

    @pytest.mark.asyncio
    async def test_suggest_mappings_calls_client(self) -> None:
        """Test suggest_mappings calls AsyncAnthropic client correctly."""
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.content = [
            MagicMock(
                text="""[
                {
                    "source_field": "order_id",
                    "target_field": "transaction_id",
                    "confidence": 0.95,
                    "reason": "ID mapping"
                }
            ]"""
            )
        ]
        # Use AsyncMock for the async messages.create method
        mock_client.messages.create = AsyncMock(return_value=mock_response)

        mapper = LLMSchemaMapper(anthropic_client=mock_client)
        profiles = {
            "order_id": ColumnProfile(
                name="order_id",
                inferred_type="string",
                total_count=1,
                null_count=0,
                unique_count=1,
                sample_values=["ORD-001"],
            ),
        }
        sample_rows = [{"order_id": "ORD-001"}]

        suggestions = await mapper.suggest_mappings(profiles, sample_rows)

        assert len(suggestions) == 1
        mock_client.messages.create.assert_called_once()
        call_args = mock_client.messages.create.call_args
        assert call_args.kwargs["model"] == "claude-sonnet-4-20250514"
        assert call_args.kwargs["max_tokens"] == 4096


class TestSchemaDiscovery:
    """Tests for SchemaDiscovery class."""

    def test_init_creates_profiler_and_mapper(self) -> None:
        """Test __init__ creates ColumnProfiler and LLMSchemaMapper."""
        discovery = SchemaDiscovery()

        assert isinstance(discovery._profiler, ColumnProfiler)
        assert isinstance(discovery._mapper, LLMSchemaMapper)

    @pytest.mark.asyncio
    async def test_analyze_empty_data_returns_empty_result(self) -> None:
        """Test analyze returns empty result structure for empty data."""
        discovery = SchemaDiscovery()

        result = await discovery.analyze([])

        assert result["profiles"] == {}
        assert result["suggestions"] == []
        assert result["field_map"] == {}
        assert result["confidence_summary"] == {"high": 0, "medium": 0, "low": 0, "unmapped": 0}

    @pytest.mark.asyncio
    async def test_analyze_returns_correct_structure(self) -> None:
        """Test analyze returns dict with profiles, suggestions, field_map, confidence_summary."""
        discovery = SchemaDiscovery()

        # Mock the mapper's suggest_mappings method
        mock_suggestions = [
            MappingSuggestion(
                source_field="order_id",
                target_field="transaction_id",
                confidence=0.95,
                reason="High confidence",
                sample_values=["ORD-001"],
            ),
            MappingSuggestion(
                source_field="amount",
                target_field="value",
                confidence=0.85,
                reason="High confidence",
                sample_values=["100.0"],
            ),
            MappingSuggestion(
                source_field="notes",
                target_field=None,
                confidence=0.3,
                reason="Low confidence",
                sample_values=["note1"],
            ),
        ]

        discovery._mapper.suggest_mappings = AsyncMock(return_value=mock_suggestions)

        data = [
            {"order_id": "ORD-001", "amount": 100.0, "notes": "note1"},
            {"order_id": "ORD-002", "amount": 200.0, "notes": "note2"},
        ]

        result = await discovery.analyze(data, context="Test data")

        assert "profiles" in result
        assert "suggestions" in result
        assert "field_map" in result
        assert "confidence_summary" in result

        assert isinstance(result["profiles"], dict)
        assert isinstance(result["suggestions"], list)
        assert isinstance(result["field_map"], dict)
        assert isinstance(result["confidence_summary"], dict)

    @pytest.mark.asyncio
    async def test_analyze_filters_high_confidence_mappings(self) -> None:
        """Test analyze filters high-confidence mappings (>= 0.7) into field_map."""
        discovery = SchemaDiscovery()

        mock_suggestions = [
            MappingSuggestion(
                source_field="order_id",
                target_field="transaction_id",
                confidence=0.95,
                reason="Very high",
                sample_values=["1"],
            ),
            MappingSuggestion(
                source_field="amount",
                target_field="value",
                confidence=0.7,
                reason="At threshold",
                sample_values=["100"],
            ),
            MappingSuggestion(
                source_field="notes",
                target_field="product_name",
                confidence=0.69,
                reason="Below threshold",
                sample_values=["note"],
            ),
            MappingSuggestion(
                source_field="unknown",
                target_field=None,
                confidence=0.8,
                reason="No target",
                sample_values=["val"],
            ),
        ]

        discovery._mapper.suggest_mappings = AsyncMock(return_value=mock_suggestions)

        data = [{"order_id": "1", "amount": 100, "notes": "note", "unknown": "val"}]

        result = await discovery.analyze(data)

        # Only >= 0.7 confidence AND has target_field
        assert result["field_map"] == {
            "order_id": "transaction_id",
            "amount": "value",
        }

    @pytest.mark.asyncio
    async def test_analyze_confidence_summary_counts(self) -> None:
        """Test analyze correctly counts confidence levels."""
        discovery = SchemaDiscovery()

        # Summary logic:
        # high: >= 0.7 AND target_field (changed from >= 0.8)
        # medium: 0.5 <= x < 0.7 AND target_field
        # low: 0.3 <= x < 0.5 AND target_field
        # unmapped: no target_field OR confidence < 0.3
        mock_suggestions = [
            MappingSuggestion("f1", "t1", 0.9, "high", []),  # high (>= 0.7 w/ target)
            MappingSuggestion("f2", "t2", 0.7, "high", []),  # high (>= 0.7 w/ target)
            MappingSuggestion("f3", "t3", 0.6, "medium", []),  # medium
            MappingSuggestion("f4", "t4", 0.5, "medium", []),  # medium (boundary)
            MappingSuggestion("f5", "t5", 0.4, "low", []),  # low
            MappingSuggestion("f6", None, 0.8, "unmapped", []),  # unmapped (no target)
            MappingSuggestion("f7", "t7", 0.2, "unmapped", []),  # unmapped (< 0.3)
        ]

        discovery._mapper.suggest_mappings = AsyncMock(return_value=mock_suggestions)

        data = [{"f1": 1, "f2": 2, "f3": 3, "f4": 4, "f5": 5, "f6": 6, "f7": 7}]

        result = await discovery.analyze(data)

        assert result["confidence_summary"]["high"] == 2  # >= 0.7 with target
        assert result["confidence_summary"]["medium"] == 2  # 0.5 <= x < 0.7 with target
        assert result["confidence_summary"]["low"] == 1  # 0.3 <= x < 0.5 with target
        assert result["confidence_summary"]["unmapped"] == 2  # no target OR < 0.3

    @pytest.mark.asyncio
    async def test_analyze_passes_context_to_mapper(self) -> None:
        """Test analyze passes context to mapper.suggest_mappings."""
        discovery = SchemaDiscovery()

        discovery._mapper.suggest_mappings = AsyncMock(return_value=[])

        data = [{"field": "value"}]
        context = "Toast POS data"

        await discovery.analyze(data, context=context)

        discovery._mapper.suggest_mappings.assert_called_once()
        call_args = discovery._mapper.suggest_mappings.call_args
        # Context is passed positionally, not as keyword
        assert context in call_args.args or call_args.kwargs.get("context") == context

    @pytest.mark.asyncio
    async def test_analyze_profiles_data_before_mapping(self) -> None:
        """Test analyze profiles data using ColumnProfiler before mapping."""
        discovery = SchemaDiscovery()

        discovery._mapper.suggest_mappings = AsyncMock(return_value=[])

        data = [
            {"order_id": "ORD-001", "amount": 100.0},
            {"order_id": "ORD-002", "amount": 200.0},
        ]

        result = await discovery.analyze(data)

        # Verify profiles were created
        assert "order_id" in result["profiles"]
        assert "amount" in result["profiles"]
        assert result["profiles"]["order_id"].name == "order_id"
        assert result["profiles"]["amount"].name == "amount"

        # Verify mapper was called with profiles
        discovery._mapper.suggest_mappings.assert_called_once()
        call_args = discovery._mapper.suggest_mappings.call_args
        # Profiles are passed positionally as first arg
        profiles_arg = call_args.args[0] if call_args.args else call_args.kwargs.get("source_profiles")
        assert "order_id" in profiles_arg
        assert "amount" in profiles_arg
