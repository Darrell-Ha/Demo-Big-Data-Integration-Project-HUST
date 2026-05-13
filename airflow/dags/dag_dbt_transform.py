from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from datetime import timedelta

DAG_ID = "dbt_backfill_silver_gold_sequential"

DBT_PROFILES_DIR = Variable.get("OLIST_DBT_PROFILES_DIR", default_var="/opt/airflow/dbt")
DBT_PROJECT_DIR = Variable.get("OLIST_DBT_PROJECT_DIR", default_var="/opt/airflow/dbt/project")

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Sequential dbt backfill Olist Lakehouse bronze -> silver -> gold",
    schedule_interval=None,     # manual trigger
    start_date=days_ago(1),
    catchup=False,
    params={
        "start_date": None,
        "end_date": None,
        "extra_dbt_args": ""     # ví dụ: --full-refresh
    },
    tags=["dbt", "backfill", "sequential"],
) as dag:
    
    start = EmptyOperator(task_id="start")

    test_dq_olist_bronze = BashOperator(
        task_id="test_dq_olist_bronze",
        cwd="/opt/airflow/scripts",
        bash_command="""
            ./dbt-test.sh --select source:olist_bronze
        """,
        env={
            "DBT_PROFILES_DIR": DBT_PROFILES_DIR,
            "DBT_PROJECT_DIR": DBT_PROJECT_DIR
        },
    )

    # 1️⃣ SILVER
    backfill_olist_silver = BashOperator(
        task_id="backfill_olist_silver",
        cwd="/opt/airflow/scripts",
        bash_command="""
        ./dbt-run.sh backfill \
          --start-date {{ params.start_date }} \
          --end-date {{ params.end_date }} \
          --select tag:olist_silver \
          {{ params.extra_dbt_args }}
        """,
        env={
            "DBT_PROFILES_DIR": DBT_PROFILES_DIR,
            "DBT_PROJECT_DIR": DBT_PROJECT_DIR
        },
    )

    test_dq_olist_silver = BashOperator(
        task_id="test_dq_olist_silver",
        cwd="/opt/airflow/scripts",
        bash_command="""
            ./dbt-test.sh --select tag:olist_silver
        """,
        env={
            "DBT_PROFILES_DIR": DBT_PROFILES_DIR,
            "DBT_PROJECT_DIR": DBT_PROJECT_DIR
        },
    )

    # 2️⃣ GOLD
    backfill_olist_gold = BashOperator(
        task_id="backfill_olist_gold",
        cwd="/opt/airflow/scripts",
        bash_command="""
        ./dbt-run.sh backfill \
          --start-date {{ params.start_date }} \
          --end-date {{ params.end_date }} \
          --select tag:olist_gold \
          {{ params.extra_dbt_args }}
        """,
        env={
            "DBT_PROFILES_DIR": DBT_PROFILES_DIR,
            "DBT_PROJECT_DIR": DBT_PROJECT_DIR
        },
    )

    test_dq_olist_gold = BashOperator(
        task_id="test_dq_olist_gold",
        cwd="/opt/airflow/scripts",
        bash_command="""
            ./dbt-test.sh --select tag:olist_gold
        """,
        env={
            "DBT_PROFILES_DIR": DBT_PROFILES_DIR,
            "DBT_PROJECT_DIR": DBT_PROJECT_DIR
        },
    )

    end = EmptyOperator(task_id="end")

    # 🔗 dependency
    start >> test_dq_olist_bronze >> backfill_olist_silver >> test_dq_olist_silver >> backfill_olist_gold >> test_dq_olist_gold >> end