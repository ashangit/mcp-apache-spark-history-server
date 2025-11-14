from datetime import datetime
from typing import Optional

"""
Yoshi API client for retrieving job definitions and metadata.

This module provides a client for interacting with the Yoshi job orchestration
platform, which manages Spark jobs within Datadog's infrastructure.
"""

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
    """
    Client for interacting with the Yoshi job orchestration API.
    
    Yoshi is Datadog's internal job orchestration platform that manages Spark
    jobs. This client provides authenticated access to retrieve job definitions,
    metadata, and configuration.
    
    Attributes:
        AUDIENCE: The service audience identifier for JWT authentication
        configuration: Yoshi API client configuration with authentication
        
    Example:
        >>> yoshi = Yoshi(datacenter="us1")
        >>> job = yoshi.get_job_definition("my-spark-job-123")
        >>> print(f"Job name: {job.name}")
    """
    
    AUDIENCE = "rapid-data-eng-infra"
    LIST_LIMIT = 100

    def __init__(self, datacenter: str):

        """
        Initialize the Yoshi API client with datacenter-specific configuration.
        
        Creates a Yoshi API client configured for the specified datacenter. When
        running inside a Kubernetes pod (POD_NAME is set), uses the internal
        cluster service endpoint. Otherwise, uses the external datacenter endpoint.
        Automatically handles JWT authentication using the service account.
        
        Args:
            datacenter: The datacenter identifier (e.g., "us1", "eu1", "us5")
                       This determines which Yoshi endpoint to connect to.
                       
        Example:
            >>> # For US datacenter
            >>> yoshi_us = Yoshi(datacenter="us1")
            >>> 
            >>> # For EU datacenter
            >>> yoshi_eu = Yoshi(datacenter="eu1")
            
        Note:
            - Inside k8s pods: uses internal fabric service mesh endpoint
            - Outside k8s: uses public datacenter-specific endpoint
            - Authentication is handled automatically via JWT tokens
        """
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


    def get_job_definition(self, job_id: str) -> Job:
        """
        Retrieve a job definition from Yoshi by job ID.
        
        Fetches the complete job definition including configuration, schedule,
        Spark settings, and metadata for a specific Yoshi job. This is useful
        for investigating job failures, analyzing configurations, or retrieving
        Spark application IDs associated with job runs.
        
        Args:
            job_id: The unique identifier for the Yoshi job (e.g., "my-etl-job")
                   This is the job name/ID used when the job was created in Yoshi.
                   
        Returns:
            Job object containing complete job definition including:
                - Job configuration (Spark settings, resources, etc.)
                - Schedule information
                - Execution history
                - Associated Spark application IDs
                - Job metadata and tags
                
        Raises:
            ApiException: If the job_id doesn't exist or API request fails
            AuthenticationError: If JWT authentication fails
            
        Example:
            >>> yoshi = Yoshi(datacenter="us1")
            >>> job = yoshi.get_job_definition("daily-etl-pipeline")
            >>> print(f"Job: {job.name}")
            >>> print(f"Schedule: {job.schedule}")
            >>> print(f"Spark app: {job.spark_application_id}")
            
        Note:
            This method requires valid JWT authentication and appropriate
            permissions to access the specified job in Yoshi.
        """

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
