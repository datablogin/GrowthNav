"""Tests for IdentityLinker resource cleanup and context manager."""

from __future__ import annotations

from growthnav.connectors.identity import IdentityLinker


class TestIdentityLinkerCleanup:
    """Tests for IdentityLinker resource cleanup and context manager."""

    def test_context_manager_basic_usage(self) -> None:
        """Test using IdentityLinker as a context manager."""
        with IdentityLinker() as linker:
            linker.add_records(
                [{"id": "1", "email": "test@example.com"}],
                source="test",
            )
            identities = linker.resolve_deterministic()
            assert len(identities) == 1

        # After context exit, resources should be cleaned up
        assert linker._linker is None
        assert len(linker._records) == 0
        assert linker._model_trained is False

    def test_close_method_clears_records(self) -> None:
        """Test close() method clears internal state."""
        linker = IdentityLinker()
        linker.add_records(
            [{"id": "1", "email": "test@example.com"}],
            source="test",
        )

        assert len(linker._records) == 1

        linker.close()

        assert len(linker._records) == 0
        assert linker._linker is None
        assert linker._model_trained is False

    def test_close_is_idempotent(self) -> None:
        """Test close() can be called multiple times safely."""
        linker = IdentityLinker()
        linker.add_records(
            [{"id": "1", "email": "test@example.com"}],
            source="test",
        )

        # Close multiple times should not raise
        linker.close()
        linker.close()
        linker.close()

        assert len(linker._records) == 0

    def test_close_handles_linker_without_db_api(self) -> None:
        """Test close() handles linker that doesn't have db_api attribute."""
        linker = IdentityLinker()

        # Mock a linker without db_api
        class MockLinker:
            pass

        linker._linker = MockLinker()

        # Should not raise even though linker has no db_api
        linker.close()

        assert linker._linker is None

    def test_close_handles_db_api_close_exception(self) -> None:
        """Test close() handles exceptions from db_api.close() gracefully."""
        linker = IdentityLinker()

        # Mock a linker with db_api that raises on close
        class MockDBAPI:
            def close(self) -> None:
                raise RuntimeError("Failed to close")

        class MockLinker:
            db_api = MockDBAPI()

        linker._linker = MockLinker()

        # Should not raise, just log warning
        linker.close()

        assert linker._linker is None

    def test_context_manager_returns_self(self) -> None:
        """Test __enter__ returns self."""
        linker = IdentityLinker()
        result = linker.__enter__()
        assert result is linker

    def test_context_manager_calls_close_on_exit(self) -> None:
        """Test __exit__ calls close() method."""
        linker = IdentityLinker()
        linker.add_records([{"id": "1", "email": "test@example.com"}], source="test")

        linker.__enter__()
        linker.__exit__(None, None, None)

        # Verify close was called
        assert len(linker._records) == 0
        assert linker._linker is None
