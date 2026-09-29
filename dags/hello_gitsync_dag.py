"""
과제 04 — git-sync 동작 확인용 테스트 DAG (BashOperator 만 사용)
작성자: 정혜정 (데엔 7기)
"""
import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="hello_gitsync",
    description="git-sync 로 배포됐는지 확인하는 테스트 DAG",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    default_args={"owner": "정혜정"},
    tags=["gitsync", "q4", "week8", "정혜정"],
) as dag:

    say_hello = BashOperator(
        task_id="say_hello",
        bash_command='echo "hello from git-sync — 정혜정 (데엔 7기) $(date)"; hostname',
    )
