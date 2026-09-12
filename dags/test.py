from datetime import datetime

from airflow.sdk import DAG
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig


DBT_PROJECT_PATH = "/usr/local/airflow/dbt_demo"
DBT_PROFILES_PATH = "/usr/local/airflow/dbt_demo/dbt_profiles/profiles.yml"


profile_config = ProfileConfig(
    profile_name="dbt_demo",
    target_name="dev",
    profiles_yml_filepath=DBT_PROFILES_PATH,
)


with DAG(
    dag_id="dbt_demo_pipeline",
    start_date=datetime(2026, 9, 11),
    schedule=None,
    catchup=False,
    tags=["dbt", "sqlserver"],
) as dag:

    dbt_run = DbtTaskGroup(
        group_id="dbt_run",
        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT_PATH,
        ),
        profile_config=profile_config,
    )