from airflow import DAG
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

from datetime import datetime, timedelta
from pathlib import Path
import csv


# --------------------------------------------------
# CSV FOLDER
# --------------------------------------------------

INPUT_FOLDER = Path(
    "/usr/local/airflow/data/filepath"
)


# --------------------------------------------------
# CHECK FOR CSV
# --------------------------------------------------

def check_for_csv():

    files = list(
        INPUT_FOLDER.glob("*.csv")
    )

    if files:

        print("CSV files found:")

        for file in files:
            print(file)

        return True

    print("No CSV files found.")

    return False


# --------------------------------------------------
# LOAD CSV TO BRONZE
# --------------------------------------------------

def load_files_to_bronze():

    files = list(
        INPUT_FOLDER.glob("*.csv")
    )

    if not files:

        print("No CSV files to process.")

        return


    # ----------------------------------------------
    # SQL SERVER CONNECTION
    # ----------------------------------------------

    hook = MsSqlHook(
        mssql_conn_id="SSMS"
    )

    conn = hook.get_conn()

    cursor = conn.cursor()


    # ----------------------------------------------
    # PROCESS EACH CSV
    # ----------------------------------------------

    for file in files:

        file_name = file.name

        print(
            f"Checking file: {file_name}"
        )


        # ------------------------------------------
        # CHECK FILE AUDIT
        # ------------------------------------------

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM bronze.File_Audit
            WHERE FileName = %s
            """,
            (file_name,)
        )

        already_loaded = cursor.fetchone()[0]


        # ------------------------------------------
        # SKIP IF ALREADY LOADED
        # ------------------------------------------

        if already_loaded > 0:

            print(
                f"Skipping {file_name} "
                f"- already loaded."
            )

            continue


        # ------------------------------------------
        # READ CSV
        # ------------------------------------------

        rows = []

        with open(
            file,
            mode="r",
            encoding="utf-8-sig",
            newline=""
        ) as csv_file:

            reader = csv.DictReader(
                csv_file
            )

            for row in reader:

                rows.append(
                    (
                        int(row["CustomerID"]),
                        row["CustomerName"],
                        row["Email"],
                        row["City"],
                        file_name
                    )
                )


        print(
            f"{file_name}: "
            f"{len(rows)} rows found."
        )


        # ------------------------------------------
        # LOAD INTO BRONZE
        # ------------------------------------------

        cursor.executemany(
            """
            INSERT INTO bronze.Customer_Raw
            (
                CustomerID,
                CustomerName,
                Email,
                City,
                SourceFile
            )
            VALUES (%s, %s, %s, %s, %s)
            """,
            rows
        )


        # ------------------------------------------
        # FILE AUDIT
        # ------------------------------------------

        cursor.execute(
            """
            INSERT INTO bronze.File_Audit
            (
                FileName,
                RowsLoaded,
                Status
            )
            VALUES (%s, %s, %s)
            """,
            (
                file_name,
                len(rows),
                "SUCCESS"
            )
        )


        # ------------------------------------------
        # COMMIT
        # ------------------------------------------

        conn.commit()


        print(
            f"{file_name} "
            f"loaded successfully."
        )


    cursor.close()

    conn.close()


# --------------------------------------------------
# DAG
# --------------------------------------------------

with DAG(

    dag_id="csv_to_bronze",

    start_date=datetime(
        2026,
        9,
        12
    ),

    schedule="*/5 * * * *",

    catchup=False,

    default_args={

        "owner": "data-engineering",

        "retries": 2,

        "retry_delay": timedelta(
            minutes=2
        ),

    },

    tags=[
        "csv",
        "bronze",
        "sqlserver"
    ],

) as dag:


    # Wait for CSV
    wait_for_csv = PythonSensor(

        task_id="wait_for_csv",

        python_callable=check_for_csv,

        poke_interval=30,

        timeout=3600,

        mode="reschedule",

    )


    # Load CSV
    load_to_bronze = PythonOperator(

        task_id="load_to_bronze",

        python_callable=load_files_to_bronze,

    )


    wait_for_csv >> load_to_bronze