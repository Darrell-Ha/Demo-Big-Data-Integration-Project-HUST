"""
dags/dag_bronze_ingestion.py
 
DAG điều phối Step 1: ingest toàn bộ 9 bảng Olist từ S3 CSV → Bronze Iceberg.
 
"""

import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from setup_vars import (
    AIRFLOW_HOME,
    S3_ENDPOINT_URL, S3_BUCKET_NAME, S3_ACCESS_KEY, S3_SECRET_KEY,
    THRIFT_HIVE_URI, ICEBERG_HIVE_CATALOG_NAME,
    create_spark_iceberg_config
)
from olist_retail_lakehouse.spark_jobs.init_setup.logger import log
from olist_retail_lakehouse.spark_jobs.ingestion.models_stg_tbls import CSV_FILES

# ---------------------------------------------------------------------------
# Config — lấy từ Airflow Variables (set qua UI hoặc CLI)
# ---------------------------------------------------------------------------
 
SPARK_CONN_ID   = "retail_spark_cluster"          # Airflow Connection ID cho Spark
S3_CONN_ID      = "s3_minio_storage"            # Airflow Connection ID cho S3/MinIO
S3_BUCKET       = S3_BUCKET_NAME
STAGING_PREFIX  = "staging"
SPARK_JOB_PATH  = f"{AIRFLOW_HOME}/dags/jobs/job_ingestion_stg_to_bronze.py"
 

def make_spark_ingest_task(dag: DAG, table: str) -> SparkSubmitOperator:
    """
    Tạo SparkSubmitOperator cho 1 bảng.
    batch_id dùng execution_date của Airflow để idempotent.
    """
    return SparkSubmitOperator(
        task_id         = f"ingest_{table}",
        name            = f"bronze_ingestion_{table}",
        application     = SPARK_JOB_PATH,
        conn_id         = SPARK_CONN_ID,
        application_args= [
            "--table",    table,
            "--batch_id", "{{ ds_nodash }}_{{ run_id | replace(':', '_') | truncate(20, True, '') }}",
        ],
        conf            = create_spark_iceberg_config(),
        dag             = dag,
        do_xcom_push    = True,     # push metrics từ PySpark stdout vào XCom
        env_vars        = {
            "S3_BUCKET_NAME": S3_BUCKET_NAME,
            "S3_ACCESS_KEY": S3_ACCESS_KEY,
            "S3_SECRET_KEY": S3_SECRET_KEY,
            "S3_ENDPOINT_URL": S3_ENDPOINT_URL,
            "THRIFT_HIVE_URI": THRIFT_HIVE_URI,
            "ICEBERG_HIVE_CATALOG_NAME": ICEBERG_HIVE_CATALOG_NAME
        },
        py_files         = f"{AIRFLOW_HOME}/dags/resources_spark_submit/olist_retail_lakehouse.zip",
    )

# ---------------------------------------------------------------------------
# DQ Summary task
# ---------------------------------------------------------------------------
 
def summarize_dq_results(**context) -> None:
    """
    Pull metrics từ XCom của tất cả ingest tasks, log summary.
    Raise AirflowException nếu bất kỳ bảng nào có quarantine_rows vượt ngưỡng.
    """
    ti = context["ti"]
    all_tables = list(CSV_FILES.keys())
 
    summary = {}
    has_critical_failure = False
    QUARANTINE_THRESHOLD = 0.05  # 5% quarantine rate được phép
 
    for table in all_tables:
        # SparkSubmitOperator push stdout — metrics được log ở đây để visibility
        # Trong production, nên ghi metrics vào audit table qua PySpark job
        xcom_val = ti.xcom_pull(task_ids=f"ingest_{table}")
        if xcom_val:
            try:
                metrics = json.loads(xcom_val) if isinstance(xcom_val, str) else xcom_val
                summary[table] = metrics
 
                total = metrics.get("total_raw_rows", 0)
                quar  = metrics.get("quarantine_rows", 0)
                if total > 0:
                    qrate = quar / total
                    if qrate > QUARANTINE_THRESHOLD:
                        log.error(
                            f"[DQ FAIL] {table}: quarantine rate {qrate:.2%} "
                            f"vượt ngưỡng {QUARANTINE_THRESHOLD:.0%}"
                        )
                        has_critical_failure = True
                    else:
                        log.info(f"[DQ OK] {table}: quarantine rate {qrate:.2%}")
 
            except Exception as e:
                log.warning(f"Không parse được metrics cho {table}: {e}")
 
    log.info("=== DQ Summary ===")
    for table, m in summary.items():
        log.info(f"  {table}: total={m.get('total_raw_rows')} | "
                 f"valid={m.get('valid_rows')} | "
                 f"quarantine={m.get('quarantine_rows')} | "
                 f"written={m.get('written_rows')}")
 
    if has_critical_failure:
        raise ValueError(
            "DQ check thất bại: một hoặc nhiều bảng có quarantine rate vượt ngưỡng 5%. "
            "Kiểm tra bronze_quarantine tables và logs."
        )
    
with DAG(
    dag_id            = "stg_ingestion_to_bronze",
    description       = "Step 1: Ingest Olist CSV từ S3 sang Bronze Iceberg via PySpark",
    start_date        = days_ago(1),
    schedule_interval = None,               # trigger thủ công hoặc từ upstream DAG
    catchup           = False,
    max_active_tasks  = 2,                  # không chạy song song 2 pipeline
    tags              = ["bronze", "ingestion", "olist", "iceberg"],
    render_template_as_native_obj = True,
) as dag:
 
    # ------------------------------------------------------------------
    # Task: Start marker
    # ------------------------------------------------------------------
    start = EmptyOperator(task_id="start")
 
    # ------------------------------------------------------------------
    # Task group 1: S3KeySensor — kiểm tra CSV files tồn tại trên S3
    # Chạy song song tất cả sensors
    # ------------------------------------------------------------------
    sensors = {}
    for table, csv_file in CSV_FILES.items():
        sensors[table] = S3KeySensor(
            task_id         = f"check_s3_{table}",
            bucket_name     = S3_BUCKET,
            bucket_key      = f"{STAGING_PREFIX}/{csv_file}",
            aws_conn_id     = S3_CONN_ID,
            timeout         = 60 * 30,          # 30 phút timeout
            poke_interval   = 60,               # check mỗi 60 giây
            mode            = "reschedule",     # không block worker slot
            dag             = dag,
        )
        start >> sensors[table]
 
    # ------------------------------------------------------------------
    # Task group 2: Ingest các bảng độc lập (không có foreign key dependency)
    # Chạy song song sau khi tất cả sensors pass
    # ------------------------------------------------------------------
    independent_tables = [
        "orders", "customers", "products",
        "sellers", "geolocation", "product_category_name_translation"
    ]
    independent_tasks = {}
    for table in independent_tables:
        task = make_spark_ingest_task(dag, table)
        sensors[table] >> task
        independent_tasks[table] = task
 
    # ------------------------------------------------------------------
    # Task group 3: Ingest các bảng có dependency vào orders/products/sellers
    # Chạy sau khi nhóm independent hoàn tất
    # ------------------------------------------------------------------
    dependent_tables = {
        "order_items":    ["orders", "products", "sellers"],
        "order_payments": ["orders"],
        "order_reviews":  ["orders"],
    }
    dependent_tasks = {}
    for table, deps in dependent_tables.items():
        task = make_spark_ingest_task(dag, table)
        # Sensor phải pass trước
        sensors[table] >> task
        # Các bảng upstream phải ingested trước
        for dep in deps:
            independent_tasks[dep] >> task
        dependent_tasks[table] = task
 
    # ------------------------------------------------------------------
    # Task: DQ Summary — chạy sau toàn bộ ingestion
    # ------------------------------------------------------------------
    dq_summary = PythonOperator(
        task_id         = "dq_summary",
        python_callable = summarize_dq_results,
        provide_context = True,
        trigger_rule    = "all_done",   # chạy kể cả khi 1 ingest task fail
                                        # để capture toàn bộ kết quả DQ
    )
 
    end = EmptyOperator(
        task_id      = "end",
        trigger_rule = "none_failed_min_one_success",
    )
 
    # Tất cả ingest tasks đều phải xong trước dq_summary
    for task in {**independent_tasks, **dependent_tasks}.values():
        task >> dq_summary
 
    dq_summary >> end