"""Count one randomly selected Olympic medal and check the latest result in MySQL."""
import random
import time
from datetime import datetime, timezone

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.utils.trigger_rule import TriggerRule


MYSQL_CONN_ID = "goit_mysql_db"
MEDALS = ("Bronze", "Silver", "Gold")


def pick_medal():
    return random.choice(MEDALS)


def choose_branch(ti):
    medal = ti.xcom_pull(task_ids="pick_medal")
    return f"count_{medal.lower()}"


def delay_check(params):
    time.sleep(params["delay_seconds"])


with DAG(
    dag_id="zasukha_medal_counts",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={"delay_seconds": Param(2, type="integer", minimum=0, maximum=300)},
    tags=["goit", "olympics"],
) as dag:
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id=MYSQL_CONN_ID,
        sql="""
            CREATE TABLE IF NOT EXISTS zasukha_medal_counts (
                id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                medal_type VARCHAR(10) NOT NULL,
                `count` BIGINT UNSIGNED NOT NULL,
                created_at DATETIME(6) NOT NULL
            )
        """,
    )
    pick = PythonOperator(task_id="pick_medal", python_callable=pick_medal)
    branch = BranchPythonOperator(task_id="choose_branch", python_callable=choose_branch)
    delay = PythonOperator(
        task_id="delay_check",
        python_callable=delay_check,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )
    check = SqlSensor(
        task_id="check_latest_record",
        conn_id=MYSQL_CONN_ID,
        sql="""
            SELECT COALESCE(
                MAX(created_at) BETWEEN UTC_TIMESTAMP(6) - INTERVAL 30 SECOND
                                    AND UTC_TIMESTAMP(6),
                0
            )
            FROM zasukha_medal_counts
        """,
        poke_interval=2,
        timeout=6,
        mode="poke",
        soft_fail=False,
        retries=0,
    )

    create_table >> pick >> branch
    for medal in MEDALS:
        count_medal = SQLExecuteQueryOperator(
            task_id=f"count_{medal.lower()}",
            conn_id=MYSQL_CONN_ID,
            sql="""
                INSERT INTO zasukha_medal_counts (medal_type, `count`, created_at)
                SELECT %s, COUNT(*), UTC_TIMESTAMP(6)
                FROM olympic_dataset.athlete_event_results
                WHERE medal = %s
            """,
            parameters=(medal, medal),
        )
        branch >> count_medal >> delay
    delay >> check
