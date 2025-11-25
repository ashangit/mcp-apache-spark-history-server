"""
Vault and JWT authentication utilities for secure credential management.

This module provides clients for:
- JWT token generation for service-to-service authentication
- Vault secret retrieval for secure credential storage

These utilities handle both in-cluster (Kubernetes pod) and local development
authentication scenarios.
"""
import logging

from spark_history_mcp.common.variable import POD_NAME

logger = logging.getLogger(__name__)


class JWT:
    def __init__(self, audience: str, datacenter: str):
        self.audience = audience

        from dd_internal_authentication.libs.py.dd_internal_authentication.dd_internal_authentication.client import (
            JWTDDToolAuthClientTokenManager,
            JWTInternalServiceAuthClientTokenManager,
        )

        if POD_NAME:
            logger.info("Using internal service auth client")
            self.token_manager = JWTInternalServiceAuthClientTokenManager(
                issuer="sycamore"
            )
        else:
            logger.info("Using internal ddtool auth client")
            self.token_manager = JWTDDToolAuthClientTokenManager.instance(
                name=f"{self.audience}-{datacenter}", datacenter=datacenter
            )

    def get_token(self) -> str:
        return str(self.token_manager.get_token(self.audience))
