"""
GrowthNav MCP Server - Unified Model Context Protocol server.

Exposes all GrowthNav capabilities to Claude Desktop and Claude Code:
- BigQuery tools (query, schema, cost estimation)
- Reporting tools (PDF, Sheets, Slides generation)
- Customer tools (registry lookup, onboarding)
- Conversion tools (normalization, attribution)

Usage:
    # Via CLI
    growthnav-mcp

    # Via Python
    from growthnav_mcp import server
    server.run()

    # Via Claude Code (.mcp.json)
    {
        "mcpServers": {
            "growthnav": {
                "command": "uv",
                "args": ["run", "--package", "growthnav-mcp", "growthnav-mcp"]
            }
        }
    }
"""

__version__ = "0.1.0"
