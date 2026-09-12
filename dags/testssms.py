from datetime import datetime
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

with DAG(
    dag_id="test_ssms_connection",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    test_query = SQLExecuteQueryOperator(
        task_id="test_mssql_ping",
        conn_id="SSMS",  # Matches your Connection ID in the screenshot
        sql="SELECT 1 AS ConnectionTest;",
    )