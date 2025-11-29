"""Tests for growthnav_mcp package initialization."""

from __future__ import annotations


def test_version_defined():
    """Test that __version__ is defined."""
    from growthnav_mcp import __version__

    assert __version__ is not None
    assert isinstance(__version__, str)
    assert __version__ == "0.1.0"


def test_package_imports():
    """Test that the package can be imported."""
    import growthnav_mcp

    assert growthnav_mcp is not None
    assert hasattr(growthnav_mcp, "__version__")
