
from yoshi_client.domains.data_eng_infra.shared.libs.py.yoshi_client import (
    ApiClient,
    Configuration,
    Job,
    JobApi,
)

from spark_history_mcp.common.variable import POD_NAME
from spark_history_mcp.common.vault import JWT

class JobEnriched(Job):
    workflowUrl: str

class Yoshi:
    AUDIENCE = "rapid-data-eng-infra"

    def __init__(self, datacenter: str):
        host =f"https://yoshi.{datacenter}"
        if POD_NAME:
            host = f"https://yoshi.{self.AUDIENCE}.all-clusters.local-dc.fabric.dog:8443"
        self.configuration = Configuration(
            host=host,
            access_token=JWT(audience=self.AUDIENCE,datacenter=datacenter).get_token(),
        )
        self.workflow_url_prefix = f"https://temporal.{datacenter}/namespaces/v1:dataandanalytics-0/workflows"


    def get_workflow_url_for_user(self, workflow_id: str):
        return f"{self.workflow_url_prefix}/{workflow_id}"


    def get_job_definition(self, job_id: str) -> JobEnriched:
        with ApiClient(self.configuration) as api_client:
            job_api = JobApi(api_client)
            job = job_api.v2_get_job(job_id=job_id)
            return JobEnriched(**job.model_dump(), workflowUrl=self.get_workflow_url_for_user(job.state.workflow_id))


