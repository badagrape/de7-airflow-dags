"""
과제 03 — 부동산 실거래가 Gold DAG
작성자: 정혜정 (데엔 7기)

ExternalTaskSensor(silver_realestate_transform 완료 대기)
  → BashOperator + spark-submit (gold_spark_sql.py: Spark SQL 5종 집계 + write.jdbc 적재)
  → 검증 task: PostgresHook 으로 5개 테이블 row count > 0 확인
"""
import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.external_task import ExternalTaskSensor

YYYYMM = "{{ data_interval_start.in_timezone('Asia/Seoul').strftime('%Y%m') }}"

GOLD_TABLES = [
    "gold_realestate_district_avg",
    "gold_realestate_top10",
    "gold_realestate_size_dist",
    "gold_realestate_age_avg",
    "gold_realestate_mom_change",
]


def verify_row_counts():
    hook = PostgresHook(postgres_conn_id="realestate_pg")
    for table in GOLD_TABLES:
        cnt = hook.get_first(f"SELECT COUNT(*) FROM {table}")[0]
        print(f"[verify] {table}: {cnt} rows")
        if cnt <= 0:
            raise ValueError(f"{table} row count = 0")
    print("[verify] 5개 gold 테이블 모두 row count > 0 확인 완료 (정혜정)")


with DAG(
    dag_id="gold_realestate_aggregate",
    description="silver → Spark SQL 5종 집계 → PostgreSQL (정혜정)",
    schedule="@monthly",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),   # silver 와 동일
    catchup=True,
    max_active_runs=1,
    default_args={"owner": "정혜정", "retries": 1,
                  "retry_delay": pendulum.duration(minutes=2)},
    tags=["gold", "q3", "realestate", "postgres", "spark", "정혜정"],
) as dag:

    wait_for_silver = ExternalTaskSensor(
        task_id="wait_for_silver",
        external_dag_id="silver_realestate_transform",
        external_task_id=None,
        allowed_states=["success"],
        failed_states=["failed"],
        mode="reschedule",
        poke_interval=30,
        timeout=60 * 60 * 3,
    )

    spark_gold = BashOperator(
        task_id="spark_submit_gold",
        bash_command=(
            "spark-submit "
            "--master 'local[2]' "
            "--name gold_realestate_aggregate "
            "--driver-memory 1g "
            "--jars \"$SPARK_JARS\" "
            "/opt/airflow/scripts/q3/gold_spark_sql.py "
            f"--yyyymm {YYYYMM} "
            "--bucket \"$REALESTATE_BUCKET\""
        ),
    )

    verify = PythonOperator(task_id="verify_row_counts", python_callable=verify_row_counts)

    wait_for_silver >> spark_gold >> verify
