from spark_history_mcp.core.app import mcp
from mcp.server.fastmcp.prompts import base


@mcp.prompt(description="Investigate Job Issue")
def investigate_job_issue(job_id: str) -> list[base.Message]:
    return [
        base.UserMessage(f"Could you please investigate why this job {job_id} failed"),
        base.AssistantMessage(
            "I'll help find the root cause of the issue. What have you tried so far?"
        ),
    ]
