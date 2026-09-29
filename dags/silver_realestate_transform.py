"""
과제 02 — 부동산 실거래가 Silver DAG
작성자: 정혜정 (데엔 7기)

ExternalTaskSensor 로 bronze_realestate_collect 의 같은 월 DAG Run 완료를 기다린 뒤
BashOperator + spark-submit 으로 silver_spark.py 실행 (PythonOperator + python 실행 X)
"""
import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

YYYYMM = "{{ data_interval_start.in_timezone('Asia/Seoul').strftime('%Y%m') }}"

with DAG(
    dag_id="silver_realestate_transform",
    description="bronze XML → PySpark 정제 + UDF → silver parquet (정혜정)",
    schedule="@monthly",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),   # bronze 와 동일 (logical date 정렬)
    catchup=True,
    max_active_runs=1,
    default_args={"owner": "정혜정", "retries": 1,
                  "retry_delay": pendulum.duration(minutes=2)},
    tags=["silver", "q2", "realestate", "spark", "정혜정"],
) as dag:

    wait_for_bronze = ExternalTaskSensor(
        task_id="wait_for_bronze",
        external_dag_id="bronze_realestate_collect",
        external_task_id=None,               # DAG Run 전체 성공 대기
        allowed_states=["success"],
        failed_states=["failed"],
        mode="reschedule",
        poke_interval=30,
        timeout=60 * 60 * 3,
    )

    spark_silver = BashOperator(
        task_id="spark_submit_silver",
        bash_command=(
            "spark-submit "
            "--master 'local[2]' "
            "--name silver_realestate_transform "
            "--driver-memory 1g "
            "--jars \"$SPARK_JARS\" "
            "/opt/airflow/scripts/q2/silver_spark.py "
            f"--yyyymm {YYYYMM} "
            "--bucket \"$REALESTATE_BUCKET\" "
            "--hold {{ var.value.get('spark_ui_hold_seconds', 0) }}"
        ),
    )

    wait_for_bronze >> spark_silver
