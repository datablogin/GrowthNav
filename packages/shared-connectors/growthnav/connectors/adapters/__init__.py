"""Data source connector adapters.

Import this module to auto-register all available connectors.

Example:
    # Import adapters module to register all connectors
    import growthnav.connectors.adapters  # noqa: F401

    # Or import specific connectors (if their dependencies are installed)
    from growthnav.connectors.adapters.snowflake import SnowflakeConnector
"""

from __future__ import annotations

# Import adapters to trigger auto-registration
# These are conditionally imported since they have optional dependencies
try:
    from growthnav.connectors.adapters.snowflake import (
        SnowflakeConnector as SnowflakeConnector,
    )

    __all__ = ["SnowflakeConnector"]
except ImportError:
    # snowflake-connector-python not installed
    __all__ = []
