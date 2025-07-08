from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk.bases.operator import AirflowException
import glob
import pandas as pd
import pendulum

OWNER = "konstantinpavl0v"

# PG info
SCHEMA = "stg"
TABLE = "deputies"

# File info
PATH = "/tmp/deputies.csv"


def check_file_exists():
    file = glob.glob(PATH)

    if not file:
        raise AirflowException("No file found in /tmp/!")


def upload_data():
    pg_hook = PostgresHook(postgres_conn_id="postgres_duma", database="postgres_dwh")
    engine = pg_hook.get_sqlalchemy_engine()

    with pg_hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE} (
                person_id INTEGER,
                name TEXT,
                position TEXT,
                is_current BOOLEAN,
                faction_id INTEGER,
                faction_name TEXT,
                start_date DATE,
                end_date DATE
            )
            """
        )

    df = pd.read_csv(PATH)

    df.to_sql(
        con=engine,
        name=TABLE,
        schema=SCHEMA,
        if_exists="replace",
        index=False
    )


with DAG(
        dag_id="duma_deputies",
        description="This DAG is needed to get a list of deputies every month from csv and save it to Postgres",
        schedule="0 0 1 * *",
        start_date=pendulum.datetime(2025, 6, 11, tz="Europe/Moscow"),
        end_date=pendulum.datetime(2026, 6, 11, tz="Europe/Moscow"),
        default_args={"owner": OWNER, "retries": 5, "retry_delay": pendulum.duration(minutes=5)},
        tags=["api", "duma"]
) as dag:
    start = EmptyOperator(
        task_id="start"
    )

    check_file = PythonOperator(
        task_id="check_file",
        python_callable=check_file_exists
    )

    upload_data_to_pg = PythonOperator(
        task_id="upload_data_to_pg",
        python_callable=upload_data
    )

    end = EmptyOperator(
        task_id="end"
    )

    start >> check_file >> upload_data_to_pg >> end
