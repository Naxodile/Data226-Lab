"""
dbt DAG: runs AFTER the ETL DAG finishes.

schedule=None: this DAG is triggered by the last task of the ETL DAG
(see snippet at the bottom).
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.bash import BashOperator

DBT_DIR = "/opt/airflow/dbt"   # path to the dbt project (folder containing dbt_project.yml) inside the Airflow container

# Reuse the Snowflake Airflow connection so credentials aren't duplicated
conn = BaseHook.get_connection("snowflake_conn")
env = {
    "SNOWFLAKE_ACCOUNT": conn.extra_dejson.get("account", ""),
    "SNOWFLAKE_USER": conn.login,
    "SNOWFLAKE_PASSWORD": conn.password,
    "SNOWFLAKE_DATABASE": conn.extra_dejson.get("database", "ASSIGNMENTS"),
    "SNOWFLAKE_WAREHOUSE": conn.extra_dejson.get("warehouse", "COMPUTE_WH"),
    "SNOWFLAKE_SCHEMA": "ANALYTICS",
    # The mounted project folder is read-only for the airflow user, so write
    # dbt logs, compiled output and installed packages to a writable location.
    "DBT_LOG_PATH": "/tmp/dbt/logs",
    "DBT_TARGET_PATH": "/tmp/dbt/target",
    "DBT_PACKAGES_INSTALL_PATH": "/tmp/dbt/packages",
}

default_args = {"retries": 1, "retry_delay": timedelta(minutes=3)}

with DAG(
    dag_id="dbt_weather_analytics",
    start_date=datetime(2026, 1, 1),
    schedule=None,            # triggered by the ETL DAG
    catchup=False,
    default_args=default_args,
    tags=["dbt", "elt"],
) as dag:

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_DIR} && dbt deps --profiles-dir .",
        env=env, append_env=True,
    )
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && dbt run --profiles-dir .",
        env=env, append_env=True,
    )
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test --profiles-dir .",
        env=env, append_env=True,
    )
    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"cd {DBT_DIR} && dbt snapshot --profiles-dir .",
        env=env, append_env=True,
    )

    dbt_deps >> dbt_run >> dbt_test >> dbt_snapshot


# ---------------------------------------------------------------------------
# In your ETL DAG file, add this as the final task so dbt runs after ETL:
#
#   from airflow.operators.trigger_dagrun import TriggerDagRunOperator
#   trigger_dbt = TriggerDagRunOperator(
#       task_id="trigger_dbt",
#       trigger_dag_id="dbt_weather_analytics",
#       wait_for_completion=False,
#   )
#   <last_etl_task> >> trigger_dbt
# ---------------------------------------------------------------------------
