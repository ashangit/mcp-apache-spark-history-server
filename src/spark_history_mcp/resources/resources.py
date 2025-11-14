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