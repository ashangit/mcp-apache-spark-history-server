from datetime import datetime
from typing import Optional

from yoshi_client.domains.data_eng_infra.shared.libs.py.yoshi_client import (
    ApiClient,
    Configuration,
    Job,
    JobApi,
    JobType,
    Status,
)

from spark_history_mcp.common.variable import POD_NAME
from spark_history_mcp.common.vault import JWT

class JobEnriched(Job):
    workflowUrl: str

class Yoshi:
    AUDIENCE = "rapid-data-eng-infra"
    LIST_LIMIT = 100

    def __init__(self, datacenter: str):
        host = f"https://yoshi.{datacenter}"
        if POD_NAME:
            host = (
                f"https://yoshi.{self.AUDIENCE}.all-clusters.local-dc.fabric.dog:8443"
            )
        self.configuration = Configuration(
            host=host,
            access_token=JWT(audience=self.AUDIENCE, datacenter=datacenter).get_token(),
        )
        self.workflow_url_prefix = f"https://temporal.{datacenter}/namespaces/v1:dataandanalytics-0/workflows"


    def get_workflow_url_for_user(self, workflow_id: str):
        return f"{self.workflow_url_prefix}/{workflow_id}"


    def get_job_definition(self, job_id: str) -> JobEnriched:
        with ApiClient(self.configuration) as api_client:
            job_api = JobApi(api_client)
            job = job_api.v2_get_job(job_id=job_id)
            return JobEnriched(**job.model_dump(), workflowUrl=self.get_workflow_url_for_user(job.state.workflow_id))

    def list_jobs(
        self,
        statuses: Optional[list[Status]] = None,
        since: Optional[datetime] = None,
        before: Optional[datetime] = None,
        filter_subproject_name: Optional[list[str]] = None,
        filter_class_name: Optional[list[str]] = None,
        filter_team_name: Optional[list[str]] = None,
        filter_human_username: Optional[list[str]] = None,
        filter_role: Optional[list[str]] = None,
        filter_pipeline_job_id: Optional[list[str]] = None,
        filter_job_type: Optional[list[JobType]] = None,
        limits: Optional[int] = None,
    ) -> list[str]:
        with ApiClient(self.configuration) as api_client:
            job_api = JobApi(api_client)

            # Determine the page size for API calls
            page_size = self.LIST_LIMIT
            if limits is not None and limits < self.LIST_LIMIT:
                page_size = limits

            iteration = 0
            jobs: list[str] = []
            while True:
                offset = iteration * page_size
                response = job_api.v2_list_jobs(
                    limit=page_size,
                    offset=offset,
                    filter_job_type=filter_job_type,
                    filter_status=statuses,
                    filter_request_timestampge=since,
                    filter_request_timestample=before,
                    filter_subproject_name=filter_subproject_name,
                    filter_class_name=filter_class_name,
                    filter_team_name=filter_team_name,
                    filter_human_username=filter_human_username,
                    filter_role=filter_role,
                    filter_pipeline_job_id=filter_pipeline_job_id,
                    _request_timeout=5,
                    sort="request_timestamp:desc",
                )

                if response.jobs is not None:
                    jobs.extend([job.spec.job_id for job in response.jobs])

                iteration += 1

                # Stop if we've reached the requested limit or if there are no more results
                if limits is not None and len(jobs) >= limits:
                    break
                if response.details.total_count <= iteration * page_size:
                    break

            # Trim to exact limit if specified
            if limits is not None and len(jobs) > limits:
                jobs = jobs[:limits]

        return jobs
