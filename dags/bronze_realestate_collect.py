"""
과제 01 — 부동산 실거래가 Bronze (API 수집)
작성자: 정혜정 (데엔 7기)

- 국토교통부 아파트 매매 실거래가 API 를 6개 시군구에 대해 TaskGroup 으로 병렬 호출
- 응답 0건 / XML 파싱 실패 / API 오류 → BranchPythonOperator 로 skip_upload 분기
- 정상 응답은 원본 XML 그대로 s3://{bucket}/bronze/{yyyymm}/{LAWD_CD}.xml 에 boto3.put_object 로 저장

※ 키는 제출 시 빈 문자열 — docker-compose.yml 의 DATA_GO_KR_API_KEY / AWS_* 에 입력
"""
import os
import time
import xml.etree.ElementTree as ET
from datetime import datetime
from urllib.parse import unquote

import boto3
import pendulum
import requests
from botocore.exceptions import ClientError

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.task_group import TaskGroup

# ---------------------------------------------------------------------------
# 설정 (키는 빈 문자열로 제출 → 환경변수로 주입)
# ---------------------------------------------------------------------------
SERVICE_KEY = os.getenv("DATA_GO_KR_API_KEY", "")          # data.go.kr API 키
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "ap-northeast-2")
BUCKET = os.getenv("REALESTATE_BUCKET", "realestate-jeonghyejeong")
COLLECTOR_NAME = "정혜정"

API_URL = "https://apis.data.go.kr/1613000/RTMSDataSvcAptTrade/getRTMSDataSvcAptTrade"
NUM_OF_ROWS = 1000

LAWD_CODES = {
    "11680": "강남구",
    "11650": "서초구",
    "11710": "송파구",
    "11440": "마포구",
    "11170": "용산구",
    "11200": "성동구",
}


def _s3_client():
    kwargs = {"region_name": AWS_REGION}
    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
        kwargs.update(aws_access_key_id=AWS_ACCESS_KEY_ID,
                      aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    return boto3.client("s3", **kwargs)


def _call_api(lawd_cd: str, deal_ymd: str, page_no: int) -> str:
    # Encoding 키(%2B 등)를 넣어도 이중 인코딩되지 않도록 unquote
    params = {
        "serviceKey": unquote(SERVICE_KEY),
        "LAWD_CD": lawd_cd,
        "DEAL_YMD": deal_ymd,
        "pageNo": page_no,
        "numOfRows": NUM_OF_ROWS,
    }
    last_err = None
    for attempt in range(3):
        try:
            resp = requests.get(API_URL, params=params, timeout=30)
            resp.raise_for_status()
            return resp.text
        except requests.RequestException as e:
            last_err = e
            time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"API 호출 실패 lawd={lawd_cd}: {last_err}")


def collect(lawd_cd: str, **context):
    print(f'collector=정혜정, time={datetime.now()}, lawd={lawd_cd}')

    deal_ymd = context["data_interval_start"].in_timezone("Asia/Seoul").strftime("%Y%m")
    result = {"lawd_cd": lawd_cd, "deal_ymd": deal_ymd, "status": "ok", "count": 0, "xml": None}

    try:
        raw = _call_api(lawd_cd, deal_ymd, 1)
    except RuntimeError as e:
        print(f"[collect] {e}")
        result["status"] = "api_error"
        return result

    # --- XML 파싱 검증 ---
    try:
        root = ET.fromstring(raw)
    except ET.ParseError as e:
        print(f"[collect] XML 파싱 실패 lawd={lawd_cd}: {e} / 응답 앞부분={raw[:200]!r}")
        result["status"] = "parse_error"
        return result

    result_code = (root.findtext(".//resultCode") or "").strip()
    if result_code not in ("00", "000"):
        msg = root.findtext(".//resultMsg") or root.findtext(".//returnAuthMsg")
        print(f"[collect] API 오류 lawd={lawd_cd}: code={result_code} msg={msg}")
        result["status"] = "api_error"
        return result

    total = int((root.findtext(".//totalCount") or "0").strip() or 0)
    items_el = root.find(".//items")

    # 페이지가 여러 개면 나머지 페이지의 <item> 을 첫 페이지 <items> 에 이어 붙여 원본 형태 유지
    if items_el is not None and total > NUM_OF_ROWS:
        pages = (total + NUM_OF_ROWS - 1) // NUM_OF_ROWS
        for p in range(2, pages + 1):
            extra = ET.fromstring(_call_api(lawd_cd, deal_ymd, p))
            for item in extra.findall(".//item"):
                items_el.append(item)
        raw = ET.tostring(root, encoding="unicode")

    count = len(root.findall(".//item"))
    print(f"[collect] lawd={lawd_cd}({LAWD_CODES[lawd_cd]}) deal_ymd={deal_ymd} "
          f"totalCount={total} 수집건수={count}")

    if count == 0:
        result["status"] = "empty"
        return result

    result["count"] = count
    result["xml"] = raw
    return result


def _collect_results(ti):
    task_ids = [f"collect_group.collect_{c}" for c in LAWD_CODES]
    return [r for r in ti.xcom_pull(task_ids=task_ids) if r]


def branch_after_collect(**context):
    results = _collect_results(context["ti"])
    for r in results:
        print(f"[branch] lawd={r['lawd_cd']} status={r['status']} count={r['count']}")
    valid = [r for r in results if r["status"] == "ok" and r["count"] > 0]
    if not valid:
        print("[branch] 모든 시군구 응답 0건 또는 파싱 실패 → skip_upload")
        return "skip_upload"
    print(f"[branch] 정상 {len(valid)}/{len(LAWD_CODES)}개 → upload_to_s3")
    return "upload_to_s3"


def upload_to_s3(**context):
    s3 = _s3_client()
    try:
        s3.head_bucket(Bucket=BUCKET)
    except ClientError:
        print(f"[upload] 버킷 {BUCKET} 없음 → 생성")
        s3.create_bucket(Bucket=BUCKET,
                         CreateBucketConfiguration={"LocationConstraint": AWS_REGION})

    uploaded = []
    for r in _collect_results(context["ti"]):
        if r["status"] != "ok" or r["count"] == 0:
            print(f"[upload] skip lawd={r['lawd_cd']} ({r['status']})")
            continue
        key = f"bronze/{r['deal_ymd']}/{r['lawd_cd']}.xml"
        s3.put_object(Bucket=BUCKET, Key=key, Body=r["xml"].encode("utf-8"),
                      ContentType="application/xml")
        print(f"[upload] s3://{BUCKET}/{key} ({r['count']}건)")
        uploaded.append(key)
    return uploaded


def summary(**context):
    keys = context["ti"].xcom_pull(task_ids="upload_to_s3") or []
    print(f"[summary] collector={COLLECTOR_NAME} 업로드 객체 {len(keys)}개")
    for k in keys:
        print(f"  - s3://{BUCKET}/{k}")


with DAG(
    dag_id="bronze_realestate_collect",
    description="국토부 아파트 매매 실거래가 → S3 bronze (정혜정)",
    schedule="@monthly",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=True,
    max_active_runs=2,
    default_args={"owner": "정혜정", "retries": 1,
                  "retry_delay": pendulum.duration(minutes=1)},
    tags=["bronze", "q1", "realestate", "정혜정"],
) as dag:

    with TaskGroup(group_id="collect_group") as collect_group:
        for lawd in LAWD_CODES:
            PythonOperator(
                task_id=f"collect_{lawd}",
                python_callable=collect,
                op_kwargs={"lawd_cd": lawd},
            )

    branch = BranchPythonOperator(
        task_id="branch_after_collect",
        python_callable=branch_after_collect,
    )

    upload = PythonOperator(task_id="upload_to_s3", python_callable=upload_to_s3)
    skip_upload = EmptyOperator(task_id="skip_upload")
    summary_done = PythonOperator(task_id="summary_done", python_callable=summary)

    collect_group >> branch >> [upload, skip_upload]
    upload >> summary_done
