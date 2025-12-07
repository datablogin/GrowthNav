"""Data source connector adapters.

Import this module to auto-register all available connectors.

Example:
    # Import adapters module to register all connectors
    import growthnav.connectors.adapters  # noqa: F401

    # Or import specific connectors (if their dependencies are installed)
    from growthnav.connectors.adapters.snowflake import SnowflakeConnector
    from growthnav.connectors.adapters.salesforce import SalesforceConnector
    from growthnav.connectors.adapters.hubspot import HubSpotConnector
    from growthnav.connectors.adapters.zoho import ZohoConnector
"""

from __future__ import annotations

# Import adapters to trigger auto-registration
# These are conditionally imported since they have optional dependencies
try:
    from growthnav.connectors.adapters.snowflake import (
        SnowflakeConnector as SnowflakeConnector,
    )
except ImportError:
    # snowflake-connector-python not installed
    SnowflakeConnector = None  # type: ignore[assignment, misc]

try:
    from growthnav.connectors.adapters.salesforce import (
        SalesforceConnector as SalesforceConnector,
    )
except ImportError:  # pragma: no cover
    # simple-salesforce not installed
    SalesforceConnector = None  # type: ignore[assignment, misc]

try:
    from growthnav.connectors.adapters.hubspot import (
        HubSpotConnector as HubSpotConnector,
    )
except ImportError:  # pragma: no cover
    # hubspot-api-client not installed
    HubSpotConnector = None  # type: ignore[assignment, misc]

# Zoho connector uses httpx which is a core dependency
from growthnav.connectors.adapters.olo import OLOConnector as OLOConnector
from growthnav.connectors.adapters.zoho import ZohoConnector as ZohoConnector

__all__: list[str] = []

if SnowflakeConnector is not None:
    __all__.append("SnowflakeConnector")
if SalesforceConnector is not None:
    __all__.append("SalesforceConnector")
if HubSpotConnector is not None:
    __all__.append("HubSpotConnector")

__all__.append("ZohoConnector")
__all__.append("OLOConnector")
