import json
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from mcp.server.fastmcp import FastMCP

from spark_history_mcp.api.emr_persistent_ui_client import EMRPersistentUIClient
from spark_history_mcp.api.spark_client import SparkRestClient
from spark_history_mcp.config.config import Config

from ..utils.utils import ApplicationDiscovery


@dataclass
class AppContext:
    clients: dict[str, SparkRestClient]
    app_discovery: Optional[ApplicationDiscovery] = None


class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles datetime objects."""

    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


@asynccontextmanager
async def app_lifespan(server: FastMCP) -> AsyncIterator[AppContext]:
    clients: dict[str, SparkRestClient] = {}
    app_discovery = ApplicationDiscovery(clients)
    yield AppContext(
        clients=clients, app_discovery=app_discovery
    )


def run(config: Config):
    mcp.settings.host = config.mcp.address
    mcp.settings.port = int(config.mcp.port)
    mcp.settings.debug = bool(config.mcp.debug)
    mcp.run(transport=os.getenv("SHS_MCP_TRANSPORT", config.mcp.transports[0]))


mcp = FastMCP("Spark Events", lifespan=app_lifespan)

# Import tools to register them with MCP
from spark_history_mcp.tools import tools  # noqa: E402,F401
from spark_history_mcp.resources import resources  # noqa: E402,F401
from spark_history_mcp.prompts import prompts  # noqa: E402,F401
