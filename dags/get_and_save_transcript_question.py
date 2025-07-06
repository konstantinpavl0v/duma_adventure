from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.providers.http.hooks.http import HttpHook
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
import pandas as pd
import pendulum
import logging

OWNER = "konstantinpavl0v"

# API keys
API_KEY = Variable.get(
    key="duma_api_key"
)

APP_KEY = Variable.get(
    key="duma_app_key"
)

# PG info
SCHEMA = "stg"
TABLE = "transcripts"
QUESTIONS_TABLE = "questions"


def get_questions_date(**context) -> tuple[str, str]:
    questions_date = context["data_interval_start"].format("YYYY-MM-DD")

    return questions_date


def get_transcripts(**context):
    pg_hook = PostgresHook(postgres_conn_id="postgres_duma", database="postgres_dwh")
    engine = pg_hook.get_sqlalchemy_engine()
    questions_date = get_questions_date(**context)

    df = pd.read_sql(
        f"SELECT kodz, kodvopr FROM {SCHEMA}.{QUESTIONS_TABLE} WHERE datez = %s",
        con=engine,
        params=(questions_date,)
    )

    if df.empty:
        logging.info("No data to proceed the next steps. Most likely in Duma yesterday nothing happened.")
        raise AirflowSkipException("No data to proceed the next steps. Most likely in Duma yesterday nothing happened.")

    with pg_hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE} (
                date DATE,
                number INTEGER,
                maxNumber INTEGER,
                name TEXT,
                startLine INTEGER,
                endLine INTEGER,
                transcript TEXT,
                PRIMARY KEY (date, number, maxNumber, name, startLine, endLine)
            )
            """
        )

    hook = HttpHook(method="GET", http_conn_id="duma_api")

    for index, row in df.iterrows():
        kodz = row["kodz"]
        kodvopr = row["kodvopr"]

        response = hook.run(endpoint=f"/{API_KEY}/transcriptQuestion/{kodz}/{kodvopr}.json?app_token={APP_KEY}")

        response.raise_for_status()

        data = response.json()

        # The first element of the array is taken due to the API contract. The data is available only in the first element.
        meeting = data["meetings"][0]
        meeting_date = meeting["date"]
        meeting_number = meeting["number"]
        meeting_max_number = meeting["maxNumber"]
        question = meeting["questions"][0]
        question_name = question["name"]
        part = question["parts"][0]
        start_line = part["startLine"]
        end_line = part["endLine"]
        transcript_text = "\n".join(part.get("lines", []))

        with pg_hook.get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {SCHEMA}.{TABLE} (
                    date,
                    number,
                    maxNumber,
                    name,
                    startLine,
                    endLine,
                    transcript
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (date, number, maxNumber, name, startLine, endLine)
                DO UPDATE SET transcript = EXCLUDED.transcript
                """,
                (
                    meeting_date,
                    meeting_number,
                    meeting_max_number,
                    question_name,
                    start_line,
                    end_line,
                    transcript_text
                )
            )


with DAG(
        dag_id="duma_transcript_questions",
        description="This DAG is needed to get a list of transcripts of questions and save it to Postgres",
        schedule="0 0 * * *",
        start_date=pendulum.datetime(2025, 6, 11, tz="Europe/Moscow"),
        end_date=pendulum.datetime(2025, 6, 15, tz="Europe/Moscow"),
        max_active_tasks=3,
        default_args={"owner": OWNER, "retries": 5, "retry_delay": pendulum.duration(minutes=5)},
        max_active_runs=3,
        catchup=True,
        tags=["api", "duma"]
) as dag:
    start = EmptyOperator(
        task_id="start"
    )

    sensor_on_getting_questions = ExternalTaskSensor(
        task_id="sensor_on_getting_questions",
        external_dag_id="duma_questions",
        allowed_states=["success"],
        mode="reschedule",
        timeout=360000,
        poke_interval=60,
    )

    get_and_save_transcripts = PythonOperator(
        task_id="get_and_save_transcripts",
        python_callable=get_transcripts
    )

    stop = EmptyOperator(
        task_id="stop"
    )

    start >> sensor_on_getting_questions >> get_and_save_transcripts >> stop
