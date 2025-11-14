"""
Vault and JWT authentication utilities for secure credential management.

This module provides clients for:
- JWT token generation for service-to-service authentication
- Vault secret retrieval for secure credential storage

These utilities handle both in-cluster (Kubernetes pod) and local development
authentication scenarios.
"""

import logging
import os

import httpx
from httpx_retries import Retry, RetryTransport

from spark_history_mcp.common.variable import POD_NAME

logger = logging.getLogger(__name__)


class JWT:
    """
    JWT token manager for Datadog internal service authentication.
    
    Handles JWT token generation for authenticating with internal Datadog services.
    Automatically selects the appropriate authentication method based on the
    execution environment (Kubernetes pod vs local development).
    
    Attributes:
        audience: The target service audience for the JWT token
        token_manager: The underlying token manager (service or ddtool based)
        
    Example:
        >>> jwt = JWT(audience="rapid-data-eng-infra", datacenter="us1")
        >>> token = jwt.get_token()
        >>> headers = {"Authorization": f"Bearer {token}"}
    """
    
    def __init__(self, audience: str, datacenter: str):
        """
        Initialize the JWT token manager with audience and datacenter context.
        
        Configures the appropriate authentication client based on execution context:
        - Inside Kubernetes pods: Uses internal service auth with Sycamore issuer
        - Local development: Uses ddtool-based authentication
        
        Args:
            audience: The target service identifier (e.g., "rapid-data-eng-infra")
                     This identifies which service the token is intended for.
            datacenter: The datacenter identifier (e.g., "us1", "eu1", "us5")
                       Used for ddtool authentication in local development.
                       
        Example:
            >>> # For Yoshi service in US datacenter
            >>> jwt = JWT(audience="rapid-data-eng-infra", datacenter="us1")
            >>> 
            >>> # For different service
            >>> jwt = JWT(audience="my-service", datacenter="eu1")
            
        Note:
            - POD_NAME environment variable determines the authentication method
            - In pods: requires Sycamore service account setup
            - Locally: requires ddtool configuration
        """
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
                name=self.audience, datacenter=datacenter
            )

    def get_token(self) -> str:
        """
        Retrieve a valid JWT token for the configured audience.
        
        Obtains a JWT token from the token manager. The token manager handles
        token caching and automatic renewal, so this method can be called
        repeatedly without concerns about rate limiting or token expiration.
        
        Returns:
            A valid JWT token string that can be used for authentication
            
        Example:
            >>> jwt = JWT(audience="rapid-data-eng-infra", datacenter="us1")
            >>> token = jwt.get_token()
            >>> 
            >>> # Use token in API requests
            >>> import requests
            >>> response = requests.get(
            ...     "https://api.internal/v1/jobs",
            ...     headers={"Authorization": f"Bearer {token}"}
            ... )
            
        Note:
            - Tokens are cached and automatically renewed by the token manager
            - This method is thread-safe
            - Token validity is typically 1 hour
        """
        return str(self.token_manager.get_token(self.audience))


class VaultApi:
    """
    Client for retrieving secrets from HashiCorp Vault.
    
    Provides secure access to secrets stored in Vault's KV v2 (key-value version 2)
    secret store. Automatically handles authentication differences between Kubernetes
    pod and local development environments.
    
    Attributes:
        VAULT_URL: The Vault agent endpoint URL (configurable via VAULT_ADDR env var)
        DEFAULT_TIMEOUT_SECONDS: Request timeout for Vault API calls (30 seconds)
        transport: HTTP transport with retry logic (5 retries with backoff)
        
    Example:
        >>> vault = VaultApi()
        >>> api_key = vault.get_secret_kv_store(
        ...     "k8s/prod/my-service/datadog", 
        ...     "dd_api_key"
        ... )
        >>> print(f"Retrieved API key: {api_key[:4]}...")
        
    Note:
        - In Kubernetes pods: Uses pod service account for authentication
        - Locally: Requires VAULT_TOKEN environment variable
        - Implements automatic retries with exponential backoff
    """
    
    VAULT_URL = os.environ.get("VAULT_ADDR", "http://127.0.0.1:8658/vault/agent")
    DEFAULT_TIMEOUT_SECONDS = 30

    def __init__(self):
        """
        Initialize the Vault API client with retry logic.
        
        Creates an HTTP transport with automatic retry capabilities. Retries are
        configured with exponential backoff to handle transient network issues
        or temporary Vault unavailability.
        
        Retry Configuration:
            - Total retries: 5
            - Backoff factor: 0.5 (delays: 0.5s, 1s, 2s, 4s, 8s)
            - Retries on connection errors and 5xx responses
            
        Example:
            >>> vault = VaultApi()
            >>> # Client is now ready to fetch secrets
        """
        self.transport = RetryTransport(retry=Retry(total=5, backoff_factor=0.5))

    def get_secret_kv_store(self, secret_path: str, key: str) -> str | None:
        """
        Fetch a specific key from a Vault KV v2 secret store.
        
        Retrieves a secret value from Vault's key-value version 2 store. The KV v2
        store supports versioning and metadata, with secrets stored under a nested
        data structure. This method handles authentication automatically based on
        the execution environment.
        
        Args:
            secret_path: The path to the secret in Vault (without 'kv/data/' prefix)
                        Example: "k8s/prod/my-service/credentials"
            key: The specific key within the secret to retrieve
                Example: "api_key", "password", "token"
                
        Returns:
            The secret value as a string, or None if:
                - The secret path doesn't exist
                - The key is not found within the secret
                - The secret has no data
                
        Raises:
            httpx.HTTPStatusError: If Vault returns an error (403, 404, 500, etc.)
            httpx.RequestError: If network/connection issues occur after all retries
            
        Example:
            >>> vault = VaultApi()
            >>> 
            >>> # Retrieve Datadog API credentials
            >>> api_key = vault.get_secret_kv_store(
            ...     "k8s/prod/spark-mcp/datadog",
            ...     "dd_api_key"
            ... )
            >>> 
            >>> app_key = vault.get_secret_kv_store(
            ...     "k8s/prod/spark-mcp/datadog",
            ...     "dd_app_key"
            ... )
            >>> 
            >>> if api_key and app_key:
            ...     print("Successfully retrieved credentials")
            ... else:
            ...     print("Missing credentials")
                
        Note:
            - Uses KV v2 API format: /v1/kv/data/{secret_path}
            - Secrets have nested structure: data.data.{key}
            - In pods: Authentication via X-Vault-Request header
            - Locally: Authentication via X-Vault-Token header (requires VAULT_TOKEN env)
            - Reference: https://datadoghq.atlassian.net/wiki/spaces/RUNTIME/pages/2701559033/Vault
        """
        logger.info(
            "Fetching secret from Vault",
            extra={"kv_backend": "kv", "path": secret_path},
        )

        headers = {"X-Vault-Request": "true"}
        if POD_NAME is None:
            headers["X-Vault-Token"] = os.getenv("VAULT_TOKEN")
        with httpx.Client(transport=self.transport) as client:
            response = client.get(
                f"{self.VAULT_URL}/v1/kv/data/{secret_path}",
                headers=headers,
                timeout=self.DEFAULT_TIMEOUT_SECONDS,
            )
            response.raise_for_status()

            # kv-v2 has a nested structure, see
            # https://www.vaultproject.io/api/secret/kv/kv-v2#read-secret-version for an example
            secret_data = response.json().get("data", {}).get("data", None)

            if not secret_data:
                logger.warning(
                    f"Could not find secret data in kv-v2 store for path: {secret_path}"
                )
                return None

            return secret_data.get(key, None)
