from ..common.variable import DD_DATACENTER


def get_spark_history_server_url_for_user(app_id: str, datacenter:str = DD_DATACENTER):
    return f"https://spark-history-server.{datacenter}/history/{app_id}/jobs/"
