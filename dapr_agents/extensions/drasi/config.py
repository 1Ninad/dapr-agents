"""
Configuration dataclass for Drasi integration in Dapr Agents.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class DrasiSubscriptionConfig:
    """
    Optional configuration for Drasi integration on a Dapr Agent.

    Use this if you want to connect your agent to the Router Reaction's
    MCP server at startup to dynamically discover available queries.

    Example:
        from dapr_agents.extensions.drasi import DrasiSubscriptionConfig
        from dapr_agents.tool.mcp.client import MCPClient

        drasi_config = DrasiSubscriptionConfig(
            router_mcp_url="http://drasi-router:3001/mcp"
        )

        # Then connect the agent's MCPClient to the router:
        mcp = MCPClient()
        await mcp.connect_streamable_http("drasi-router", url=drasi_config.router_mcp_url)
    """
    default_pubsub: str = "agent-pubsub"
    router_mcp_url: Optional[str] = None
