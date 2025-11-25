import logging
import os
from datetime import datetime
from functools import cached_property

from datadog_api_client import Configuration, ApiClient
from datadog_api_client.v2.api.events_api import EventsApi
from datadog_api_client.v2.api.logs_api import LogsApi
from datadog_api_client.v2.model.events_sort import EventsSort
from datadog_api_client.v2.model.logs_list_request import LogsListRequest
from datadog_api_client.v2.model.logs_list_request_page import LogsListRequestPage
from datadog_api_client.v2.model.logs_query_filter import LogsQueryFilter
from datadog_api_client.v2.model.logs_sort import LogsSort
from pydantic import BaseModel, Field


logger = logging.getLogger(__name__)


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


class Datadog:
    LIMIT_PER_QUERY_LOGS = 1000
    MAX_RETURN_LOGS = 100000

    def __init__(self, datacenter: str):
        if not os.environ.get("DD_API_KEY"):
            logger.error("DD_API_KEY environment variable not set")
            raise RuntimeError("DD_API_KEY environment variable not set")

        if not os.environ.get("DD_APP_KEY"):
            logger.error("DD_APP_KEY environment variable not set")
            raise RuntimeError("DD_APP_KEY environment variable not set")

        api_key = os.environ.get("DD_API_KEY")
        app_key = os.environ.get("DD_APP_KEY")

        self.configuration = Configuration()
        self.configuration.server_variables["site"] = "datadoghq.com"
        self.configuration.api_key["apiKeyAuth"] = api_key
        self.configuration.api_key["appKeyAuth"] = app_key
        self.configuration.enable_retry = True
        self.configuration.max_retries = 5
        self.datacenter = datacenter

    @cached_property
    def base_url(self) -> str:
        base_url = "https://app.datadoghq.com"
        if "staging" in self.datacenter:
            base_url = "https://ddstaging.datadoghq.com"

        return base_url

    def list_logs(
        self, index_names: list[str], query: str, _from: datetime, to: datetime
    ) -> list[LogDD]:
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
