from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
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

def get_dates(**context) -> tuple[str, str]:
    start_date = context["data_interval_start"].format("YYYY-MM-DD")
    end_date = context["data_interval_end"].format("YYYY-MM-DD")

    return start_date, end_date

def get_and_save_questions(**context):
    hook = HttpHook(method="GET", http_conn_id="duma_api")
    pg_hook = PostgresHook(postgres_conn_id="postgres_duma", database="postgres_dwh")
    page = 1
    start_date, end_date = get_dates(**context)

    while True:
        response = hook.run(endpoint=f"/{API_KEY}/questions.json?"
                          f"dateFrom={start_date}&"
                          f"dateTo={end_date}&"
                          f"page={page}&"
                          f"app_token={APP_KEY}")

        hook.check_response(response)
        print(start_date, end_date)

        response = response.json()

        if not response['questions']:
            logging.info("No questions for this period")
            break

        with pg_hook.get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS stg.questions (
                    name TEXT,
                    datez TIMESTAMP,
                    kodz INTEGER,
                    kodvopr INTEGER,
                    nbegin INTEGER,
                    nend INTEGER
                )
                """
            )
            cur.executemany(
                """
                INSERT INTO stg.questions (name, datez, kodz, kodvopr, nbegin, nend)
                VALUES (%(name)s, %(datez)s, %(kodz)s, %(kodvopr)s, %(nbegin)s, %(nend)s)
                """,
                response["questions"]
            )

        logging.info("All data is uploaded. Go to the next page")

        page += 1


with DAG (
    dag_id="duma_questions",
    description="This DAG is needed to get a list of questions for a period and save it to Postgres",
    schedule = "0 0 * * *",
    start_date=pendulum.datetime(2025, 6, 11, tz="Europe/Moscow"),
    end_date=pendulum.datetime(2026, 6, 14, tz="Europe/Moscow"),
    max_active_tasks=3,
    default_args={"owner": OWNER, "retries": 5, "retry_delay": pendulum.duration(minutes=5)},
    max_active_runs=3,
    catchup=True,
    tags=["api", "duma"]
) as dag:
    start = EmptyOperator(
        task_id="start"
    )

    get_and_save_duma_questions = PythonOperator(
        task_id="get_and_save_duma_questions",
        python_callable=get_and_save_questions
    )

    stop = EmptyOperator(
        task_id="stop"
    )

    start >> get_and_save_duma_questions >> stop


