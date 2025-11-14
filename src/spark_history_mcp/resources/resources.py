from spark_history_mcp.core.app import mcp


@mcp.resource("confluence://Spark+Troubleshooting+Guide")
def spark_troubleshooting_guide() -> str:
    """Returns the URL to the Spark Troubleshooting Guide documentation.

    Returns:
        str: URL of the Spark Troubleshooting Guide on Confluence.
    """
    return "https://datadoghq.atlassian.net/wiki/spaces/adp/pages/2340258289/Spark+Troubleshooting+Guide"

@mcp.resource("confluence://spark-memory-settings")
def spark_memory_settings() -> str:
    """Returns the URL to the Spark Memory Settings documentation.

    Returns:
        str: URL of the Spark Memory Settings guide on Confluence.
    """
    return "https://datadoghq.atlassian.net/wiki/spaces/adp/pages/2141948557/Spark+memory+settings"

@mcp.resource("diagram://spark_workflow_sequence")
def spark_workflow_sequence() -> str:
    """Get application settings."""
    return """
    sequenceDiagram
        MortarLuigiRunner/Airflow->>+Yoshi: Create yoshi spark application;
        Yoshi->>+Spark Gateway: Create spark temporal worfklow;
        Spark Gateway->>+S3: Check availability of the build artifact (jar, pex, lnk);
        Spark Gateway->>+Kubernetes Cluster: Create spark application custom resource;
        Spark Gateway->>+Get/JobPlatform: Wait for kubernetes cluster to be selected and job to be admitted;
        Spark Operator->>+Kubernetes Cluster: Watch spark application custom resource;
        Spark Operator->>+Spark Application: Create new spark application;
        Spark Gateway->>+Kubernetes Cluster: Watch spark application custom resource to get job status;
        Spark Gateway->>+Yoshi: Update job status;
        Spark Gateway->>+ARP (AutoRemediationProcess): If job in error check if it can be remediated by ARP;
    """
