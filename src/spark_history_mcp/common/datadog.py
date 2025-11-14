import logging
from datetime import datetime, timedelta
from functools import cached_property
from threading import Lock

from datadog_api_client import Configuration, ApiClient
from datadog_api_client.v2.api.events_api import EventsApi
from datadog_api_client.v2.api.logs_api import LogsApi
from datadog_api_client.v2.model.events_sort import EventsSort
from datadog_api_client.v2.model.logs_list_request import LogsListRequest
from datadog_api_client.v2.model.logs_list_request_page import LogsListRequestPage
from datadog_api_client.v2.model.logs_query_filter import LogsQueryFilter
from datadog_api_client.v2.model.logs_sort import LogsSort
from pydantic import BaseModel, Field

from spark_history_mcp.common.variable import (
    POD_NAMESPACE,
    POD_SERVICE_ACCOUNT,
    DD_ENV,
)
from spark_history_mcp.common.vault import VaultApi

logger = logging.getLogger(__name__)

DATADOG_SECRET_KEYS = f"k8s/{POD_NAMESPACE}/{POD_SERVICE_ACCOUNT}/datadog"


class LogDD(BaseModel):
    """
    Datadog log entry model representing a single log record.
    
    This model captures the essential information from a Datadog log entry,
    including timing, content, severity, and source information.
    """
    
    timestamp: datetime = Field(description="Timestamp when the log has been emitted")
    message: str = Field(description="Log message")
    status: str = Field(description="Log level")
    host: str = Field(description="Host where the log has been emitted")
    service: str = Field(description="Service where the log has been emitted")
    pod_name: str = Field(description="Pod name where the log has been emitted")
    url: str = Field(description="URL to the individual log")


class EventDD(BaseModel):
    timestamp: datetime = Field(description="Timestamp when the event has been emitted")
    message: str = Field(description="Log message")
    pod_name: str = Field(description="Pod name where the event has been emitted")
    source: str = Field(description="Source of the event")
    url: str = Field(description="URL to the individual event")


class SingletonMeta(type):
    """
    Thread-safe Singleton metaclass.
    
    This metaclass ensures that only one instance of a class can exist at a time,
    even in multi-threaded environments. The first thread to create an instance
    acquires a lock, and subsequent calls return the existing instance.
    
    Usage:
        class MyClass(metaclass=SingletonMeta):
            pass
    """

    _instances = {}

    _lock: Lock = Lock()

    def __call__(cls, *args, **kwargs):
        """
        Control the instantiation process to ensure only one instance exists.
        
        This method is called when you call the class (e.g., MyClass()). It uses
        a lock to ensure thread-safety during instance creation.
        
        Args:
            *args: Positional arguments to pass to __init__
            **kwargs: Keyword arguments to pass to __init__
            
        Returns:
            The singleton instance of the class
            
        Note:
            Changes to __init__ arguments after the first instantiation
            do not affect the returned instance.
        """
        with cls._lock:
            if cls not in cls._instances:
                instance = super().__call__(*args, **kwargs)
                cls._instances[cls] = instance
        return cls._instances[cls]


class Datadog(metaclass=SingletonMeta):
    """
    Singleton client for interacting with the Datadog API.
    
    This class provides a unified interface for querying Datadog logs. It uses
    the Singleton pattern to ensure API credentials are loaded once and reused
    across the application. Credentials are retrieved from Vault using the
    service account context.
    
    Attributes:
        LIMIT_PER_QUERY_LOGS: Maximum number of logs to fetch per API request (1000)
        MAX_RETURN_LOGS: Maximum total number of logs to return (100000)
        configuration: Datadog API client configuration with auth credentials
        
    Example:
        >>> dd = Datadog()
        >>> logs = dd.get_logs(
        ...     index_names=["main"],
        ...     query="service:spark status:error",
        ...     _from=datetime(2024, 1, 1),
        ...     to=datetime(2024, 1, 2)
        ... )
    """
    
    LIMIT_PER_QUERY_LOGS = 1000
    MAX_RETURN_LOGS = 100000

    def __init__(self):
        """
        Initialize the Datadog client with API credentials from Vault.
        
        Retrieves the Datadog API key and application key from Vault using
        the path determined by the pod's namespace and service account. These
        credentials are used to configure the Datadog API client with retry
        logic enabled.
        
        Raises:
            Exception: If credentials cannot be retrieved from Vault
            
        Note:
            This is only called once due to the Singleton pattern, even if
            multiple Datadog() instances are requested.
        """
        vault_api = VaultApi()

        logger.info(
            f"Retrieving open lineage API Key with {DATADOG_SECRET_KEYS}: dd_api_key"
        )
        api_key = vault_api.get_secret_kv_store(DATADOG_SECRET_KEYS, "dd_api_key")
        logger.info(
            f"Retrieving open lineage API Key with {DATADOG_SECRET_KEYS}: dd_app_key"
        )
        app_key = vault_api.get_secret_kv_store(DATADOG_SECRET_KEYS, "dd_app_key")

        self.configuration = Configuration()
        self.configuration.server_variables["site"] = "datadoghq.com"
        self.configuration.api_key["apiKeyAuth"] = api_key
        self.configuration.api_key["appKeyAuth"] = app_key
        self.configuration.enable_retry = True
        self.configuration.max_retries = 5

    @cached_property
    def base_url(self) -> str:
        base_url = "https://app.datadoghq.com"
        if DD_ENV == "staging":
            base_url = "https://ddstaging.datadoghq.com"

        return base_url

    def list_logs(
        self, index_names: list[str], query: str, _from: datetime, to: datetime
    ) -> list[LogDD]:
        """
        Query Datadog logs within a specified time range.
        
        Retrieves logs from Datadog using the Logs API with automatic pagination.
        Results are filtered by the provided query string and time range. The method
        handles pagination automatically and stops when MAX_RETURN_LOGS is reached.
        
        Args:
            index_names: List of Datadog log index names to query (e.g., ["main", "prod"])
            query: Datadog query string using their search syntax
                   (e.g., "service:spark status:error host:prod-*")
            _from: Start datetime for the log search (inclusive)
            to: End datetime for the log search (inclusive)
            
        Returns:
            List of LogDD objects containing parsed log entries, sorted by timestamp
            ascending. Returns up to MAX_RETURN_LOGS (100,000) logs.
            
        Raises:
            Exception: If the Datadog API request fails or credentials are invalid
            
        Example:
            >>> dd = Datadog()
            >>> logs = dd.get_logs(
            ...     index_names=["main"],
            ...     query="service:spark-driver @spark.app_id:app-20240101-001",
            ...     _from=datetime(2024, 1, 1, 0, 0),
            ...     to=datetime(2024, 1, 1, 23, 59)
            ... )
            >>> print(f"Found {len(logs)} log entries")
            
        Note:
            - Logs are fetched in batches of LIMIT_PER_QUERY_LOGS (1000)
            - Pagination stops when MAX_RETURN_LOGS (100,000) is reached
            - If a pod_name tag is not found, the method will raise an exception
        """
        with ApiClient(self.configuration) as api_client:
            logs_api_instance = LogsApi(api_client)
            request = LogsListRequest(
                filter=LogsQueryFilter(
                    query=query,
                    indexes=index_names,
                    _from=_from.isoformat(),
                    to=to.isoformat(),
                ),
                sort=LogsSort.TIMESTAMP_ASCENDING,
                page=LogsListRequestPage(
                    limit=self.LIMIT_PER_QUERY_LOGS,
                ),
            )
            try:
                logs: list[LogDD] = []
                # Use list_logs_with_pagination for automatic pagination
                for log in logs_api_instance.list_logs_with_pagination(body=request):
                    pod_name = next(
                        (
                            tag
                            for tag in log.attributes.get("tags", [])
                            if tag.startswith("pod_name:")
                        ),
                        None,
                    ).replace("pod_name:", "")
                    logs.append(
                        LogDD(
                            timestamp=log.attributes.timestamp,
                            message=log.attributes.get("message", ""),
                            status=log.attributes.get("status", ""),
                            host=log.attributes.get("host", ""),
                            service=log.attributes.get("service", ""),
                            pod_name=pod_name,
                            url=f"{self.base_url}/logs?event={log.id}",
                        )
                    )

                    if len(logs) >= self.MAX_RETURN_LOGS:
                        break

                return logs
            except Exception as e:
                logger.error(f"Error retrieving logs: {e}")
                raise

    def list_events(self, query: str, _from: datetime, to: datetime) -> list[EventDD]:
        with ApiClient(self.configuration) as api_client:
            events_api = EventsApi(api_client)

            try:
                events = []

                # Use pagination
                for event in events_api.list_events_with_pagination(
                    filter_query=query,
                    filter_from=_from.isoformat(),
                    filter_to=to.isoformat(),
                    sort=EventsSort.TIMESTAMP_ASCENDING,
                    page_limit=self.LIMIT_PER_QUERY_LOGS,
                ):
                    pod_name = next(
                        (
                            tag
                            for tag in event.attributes.get("tags", [])
                            if tag.startswith("pod_name:")
                        ),
                        None,
                    ).replace("pod_name:", "")
                    source = next(
                        (
                            tag
                            for tag in event.attributes.get("tags", [])
                            if tag.startswith("source:")
                        ),
                        None,
                    ).replace("source:", "")
                    event_data = EventDD(
                        timestamp=event.attributes.get("timestamp", None),
                        message=event.attributes.get("message", None),
                        pod_name=pod_name,
                        source=source,
                        url=f"{self.base_url}/event/explorer?event={event.id}",
                    )

                    events.append(event_data)

                    if len(events) >= self.MAX_RETURN_LOGS:
                        break

                return events

            except Exception as e:
                logger.error(f"Error listing events (v2): {e}")
                raise
