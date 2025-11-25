import logging
import time

import httpx
from httpx_retries import RetryTransport, Retry

from spark_history_mcp.common.vault import JWT

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)
import ddtrace
import os

ddtrace.tracer.enabled = os.getenv("ENABLE_DDTRACE", False)


class Mortar:
    AUDIENCE = "mortar"
    DEFAULT_TIMEOUT_SECONDS = 30

    def __init__(self, datacenter: str):
        self.vault_addr = f"https://mortar.{datacenter}"
        self.transport = RetryTransport(retry=Retry(total=5, backoff_factor=0.5))
        self.token = JWT(audience=self.AUDIENCE, datacenter=datacenter).get_token()

    def get_job_definition(self, spark_job_id: str) -> dict:
        headers = {"authorization": f"Bearer {self.token}"}
        with httpx.Client(transport=self.transport) as client:
            response = client.get(
                f"{self.vault_addr}/v5/jobs/{spark_job_id}/children?spark_job_schema=v2",
                headers=headers,
                timeout=self.DEFAULT_TIMEOUT_SECONDS,
            )
            response.raise_for_status()

            return response.json()[0]

    def copy_logs(self, spark_app_id: str) -> bool:
        if self._copy_logs(spark_app_id):
            self._history_server_status(spark_app_id)
            return True
        return False

    def _copy_logs(self, spark_app_id: str) -> bool:
        headers = {"authorization": f"Bearer {self.token}"}
        with httpx.Client(transport=self.transport) as client:
            response = client.put(
                f"{self.vault_addr}/v4/spark_apps/{spark_app_id}/copy_logs",
                headers=headers,
                timeout=self.DEFAULT_TIMEOUT_SECONDS,
            )
            response.raise_for_status()

            return response.json()["success"]

    def _history_server_status(self, spark_app_id: str):
        headers = {"authorization": f"Bearer {self.token}"}
        while True:
            with httpx.Client(transport=self.transport) as client:
                response = client.get(
                    f"{self.vault_addr}/v4/spark_apps/{spark_app_id}/history_server_status",
                    headers=headers,
                    timeout=self.DEFAULT_TIMEOUT_SECONDS,
                )
                try:
                    response.raise_for_status()
                except Exception as e:
                    if response.status_code != 503:
                        raise e

                if response.json()["success"]:
                    return
                else:
                    logger.info(
                        "Copy of spark event log still on going. Will retry to call history server status in 10s"
                    )
                    time.sleep(10)
