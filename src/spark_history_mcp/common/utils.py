import os

DATACENTER = os.environ.get("DD_DATACENTER", "us1.staging.dog")

def get_spark_history_server_url_for_user(app_id: str, datacenter:str = DATACENTER):
    return f"https://spark-history-server.{datacenter}/history/{app_id}/jobs/"

def get_atlas_workflow_url_for_user(workflow_id: str, datacenter:str = DATACENTER):
    return f"https://temporal.{datacenter}/namespaces/v1:dataandanalytics-0/workflows/{workflow_id}"
