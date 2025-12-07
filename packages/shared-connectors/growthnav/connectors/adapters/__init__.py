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

__all__: list[str] = []

# Import adapters to trigger auto-registration
# These are conditionally imported since they have optional dependencies
try:
    from growthnav.connectors.adapters.snowflake import (
        SnowflakeConnector as SnowflakeConnector,
    )

    __all__.append("SnowflakeConnector")
except ImportError:
    # snowflake-connector-python not installed
    pass

try:
    from growthnav.connectors.adapters.salesforce import (
        SalesforceConnector as SalesforceConnector,
    )

    __all__.append("SalesforceConnector")
except ImportError:  # pragma: no cover
    # simple-salesforce not installed
    pass

try:
    from growthnav.connectors.adapters.hubspot import (
        HubSpotConnector as HubSpotConnector,
    )

    __all__.append("HubSpotConnector")
except ImportError:  # pragma: no cover
    # hubspot-api-client not installed
    pass

# Zoho connector uses httpx which is a core dependency
from growthnav.connectors.adapters.zoho import ZohoConnector as ZohoConnector

__all__.append("ZohoConnector")

# OLO connector uses httpx which is a core dependency
from growthnav.connectors.adapters.olo import OLOConnector as OLOConnector

__all__.append("OLOConnector")
