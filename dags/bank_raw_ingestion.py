from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

try:
    from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig
    COSMOS_AVAILABLE = True
except ImportError:
    COSMOS_AVAILABLE = False

INPUT_FOLDER = Path("/usr/local/airflow/data/filepath")
WINDOWS_BASE_PATH = "D:/filepath"
DBT_PROJECT_PATH = "/usr/local/airflow/dbt_demo"
DBT_PROFILES_PATH = "/usr/local/airflow/dbt_demo/dbt_profiles/profiles.yml"

CREATE_VIEW_SQL = """
CREATE OR ALTER VIEW bronze.vw_Banking_Raw_Ingest AS
SELECT 
    transaction_id, transaction_date, transaction_type, amount, currency,
    transaction_status, payment_channel, payment_method, running_balance, is_flagged_fraud,
    merchant_name, merchant_category, customer_id, customer_name, customer_dob,
    customer_gender, customer_email, customer_phone, customer_city, customer_country,
    customer_segment, kyc_status, customer_annual_income, credit_score, account_id,
    account_type, account_open_date, account_status, branch_id, branch_name,
    credit_card_id, card_type, card_network, credit_limit, card_status,
    loan_id, loan_type, loan_amount, interest_rate, tenure_months,
    loan_status, complaint_id, complaint_category, complaint_priority, complaint_status
FROM bronze.Banking_Raw;
"""

def check_for_csv():
    """
    Checks if there are any CSV files in INPUT_FOLDER that have not yet been successfully loaded.
    Prevents the sensor from falsely triggering repeatedly for already-loaded files.
    """
    files = list(INPUT_FOLDER.glob("*.csv"))
    if not files:
        print("No CSV files found in input directory.")
        return False

    hook = MsSqlHook(mssql_conn_id="SSMS")
    conn = hook.get_conn()
    cursor = conn.cursor()

    pending_files = []
    for file in files:
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM bronze.File_Audit
            WHERE FileName = %s AND Status = 'SUCCESS'
            """,
            (file.name,)
        )
        already_loaded = cursor.fetchone()[0]
        if already_loaded == 0:
            pending_files.append(file.name)

    cursor.close()
    conn.close()

    if pending_files:
        print(f"Pending CSV files found to process: {pending_files}")
        return True

    print(f"All {len(files)} CSV files in directory have already been loaded.")
    return False


def load_files_to_bronze():
    """
    Performs high-performance BULK INSERT of new CSV files into bronze.Banking_Raw.
    """
    files = list(INPUT_FOLDER.glob("*.csv"))
    if not files:
        print("No CSV files found to process.")
        return

    hook = MsSqlHook(mssql_conn_id="SSMS")
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Ensure the bulk ingest view exists
    cursor.execute(CREATE_VIEW_SQL)
    conn.commit()

    for file in files:
        file_name = file.name
        print(f"Checking status for: {file_name}")

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM bronze.File_Audit
            WHERE FileName = %s AND Status = 'SUCCESS'
            """,
            (file_name,)
        )
        if cursor.fetchone()[0] > 0:
            print(f"Skipping {file_name} - already loaded.")
            continue

        print(f"Executing BULK INSERT for {file_name}...")
        host_file_path = f"{WINDOWS_BASE_PATH}/{file_name}".replace("\\", "/")

        bulk_insert_sql = f"""
        BULK INSERT bronze.vw_Banking_Raw_Ingest
        FROM '{host_file_path}'
        WITH (
            FORMAT = 'CSV',
            FIRSTROW = 2,
            FIELDQUOTE = '"',
            TABLOCK
        );
        """

        try:
            cursor.execute(bulk_insert_sql)

            # Assign SourceFile to newly inserted rows
            cursor.execute(
                """
                UPDATE bronze.Banking_Raw
                SET SourceFile = %s
                WHERE SourceFile IS NULL
                """,
                (file_name,)
            )

            # Get count of loaded rows for this file
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM bronze.Banking_Raw
                WHERE SourceFile = %s
                """,
                (file_name,)
            )
            rows_loaded = cursor.fetchone()[0]

            cursor.execute(
                """
                INSERT INTO bronze.File_Audit (FileName, RowsLoaded, Status)
                VALUES (%s, %s, %s)
                """,
                (file_name, rows_loaded, "SUCCESS")
            )
            conn.commit()
            print(f"Successfully loaded {rows_loaded} rows from {file_name}.")

        except Exception as e:
            conn.rollback()
            cursor.execute(
                """
                INSERT INTO bronze.File_Audit (FileName, RowsLoaded, Status)
                VALUES (%s, %s, %s)
                """,
                (file_name, 0, f"FAILED: {str(e)[:450]}")
            )
            conn.commit()
            print(f"Failed to load {file_name}: {e}")
            raise e

    cursor.close()
    conn.close()


with DAG(
    dag_id="bank_raw_ingestion",
    start_date=datetime(2026, 9, 12),
    schedule=None,
    catchup=False,
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
    tags=[
        "bank_csv",
        "bronze",
        "silver",
        "gold",
        "sqlserver",
        "bulk_insert",
        "dbt"
    ],
) as dag:
    wait_for_csv = PythonSensor(
        task_id="wait_for_csv",
        python_callable=check_for_csv,
        poke_interval=30,
        timeout=3600,
        mode="reschedule",
    )

    load_to_bronze = PythonOperator(
        task_id="load_to_bronze",
        python_callable=load_files_to_bronze,
    )

    if COSMOS_AVAILABLE:
        profile_config = ProfileConfig(
            profile_name="dbt_demo",
            target_name="dev",
            profiles_yml_filepath=DBT_PROFILES_PATH,
        )
        dbt_transform = DbtTaskGroup(
            group_id="dbt_transformations",
            project_config=ProjectConfig(
                dbt_project_path=DBT_PROJECT_PATH,
            ),
            profile_config=profile_config,
        )
    else:
        dbt_transform = BashOperator(
            task_id="dbt_transformations",
            bash_command=f"cd {DBT_PROJECT_PATH} && dbt run --profiles-dir {DBT_PROJECT_PATH}/dbt_profiles",
        )

    wait_for_csv >> load_to_bronze >> dbt_transform
