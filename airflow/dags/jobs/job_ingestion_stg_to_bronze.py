import argparse
import sys
from datetime import datetime, timezone
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from olist_retail_lakehouse.spark_jobs.init_setup.logger import log
from olist_retail_lakehouse.spark_jobs.init_setup.spark_config import build_spark_session
from olist_retail_lakehouse.spark_jobs.init_setup.setup_const import (
    S3_BUCKET_NAME,
    ICEBERG_HIVE_CATALOG_NAME
)

from olist_retail_lakehouse.spark_jobs.ingestion.models_stg_tbls import (
    SCHEMAS, CSV_FILES,
    NOT_NULL_COLS, PARTITION_COLS,
    BUSINESS_KEYS
)
from olist_retail_lakehouse.spark_jobs.helpers import (
    add_metadata_columns, tag_snapshot, add_partition_date_col,
    spark_read_csv_path,
    build_null_check_condition, build_reject_reason, dedup_by_business_key
)

S3_STAGING_BASE_PATH = f"s3a://{S3_BUCKET_NAME}/staging"
S3_BRONZE_BASE_PATH = f"s3a://{S3_BUCKET_NAME}/olist-bronze"
S3_BRONZE_QUARANTINE_BASE_PATH = f"s3a://{S3_BUCKET_NAME}/olist-bronze-quarantine"

BRONZE_DB = 'olist_bronze'
QUARANTINE_DB = 'olist_bronze_quarantine'

# ---------------------------------------------------------------------------
# Core ingestion logic
# ---------------------------------------------------------------------------

def step_read_csv_stg(spark: SparkSession, table: str) -> DataFrame:
    """Đọc CSV từ S3 với schema enforcement."""
    csv_path = f"{S3_STAGING_BASE_PATH}/{CSV_FILES[table]}"
    schema   = SCHEMAS[table]

    log.info(f"Reading CSV for table '{table}' from path: {csv_path}")
    df = spark_read_csv_path(spark, csv_path, schema=schema)
    log.info(f"[{table}] Raw row count: {df.count()}")
    return df

def step_split_valid_quarantine(df: DataFrame, table: str):
    """
    Tách DataFrame thành 2:
    - valid_df:      rows pass DQ (PK not null)
    - quarantine_df: rows fail DQ (PK null hoặc không castable)
    """
    not_null_cols = NOT_NULL_COLS[table]
    reject_cond   = build_null_check_condition(not_null_cols)
    reject_reason = build_reject_reason(not_null_cols)
 
    # Cache để không đọc lại 2 lần
    df.cache()
 
    valid_df = df.filter(~reject_cond)
 
    quarantine_df = (
        df.filter(reject_cond)
        .withColumn("reject_reason",  reject_reason)
        .withColumn("reject_rule",    F.lit("pk_not_null"))
        .withColumn("rejected_at",    F.current_timestamp())
    )
 
    log.info(f"[{table}] Valid rows: {valid_df.count()} | Quarantine rows: {quarantine_df.count()}")
    return valid_df, quarantine_df

def step_write_to_bronze(df: DataFrame, table: str, spark: SparkSession) -> None:
    """
    Ghi DataFrame vào Iceberg Bronze table.
    - Nếu bảng chưa tồn tại: CREATE TABLE AS SELECT (CTAS).
    - Nếu đã tồn tại: INSERT INTO (append — không overwrite để giữ history).
    """
    iceberg_table = f"{ICEBERG_HIVE_CATALOG_NAME}.{BRONZE_DB}.{table}"
    bronze_path   = f"{S3_BRONZE_BASE_PATH}/{table}"
 
    table_exists = spark.catalog.tableExists(iceberg_table)
 
    if not table_exists:
        log.info(f"[{table}] Table không tồn tại — tạo mới: {iceberg_table}")
 
        ts_col = PARTITION_COLS.get(table)
        partition_clause = ""
        if ts_col and f"{ts_col}_date" in df.columns:
            partition_clause = f"PARTITIONED BY (days({ts_col}_date))"
 
        # CTAS qua createOrReplaceTempView + SQL để control table properties
        df.createOrReplaceTempView(f"_tmp_{table}")
        spark.sql(f"""
            CREATE TABLE {iceberg_table}
            USING iceberg
            LOCATION '{bronze_path}'
            {partition_clause}
            TBLPROPERTIES (
                'history.expire.max-snapshot-age-ms'    = '2592000000',  -- 30 days
                'history.expire.min-snapshots-to-keep'  = '30',
                'write.metadata.delete-after-commit.enabled' = 'true'
            )
            AS SELECT * FROM _tmp_{table}
        """)
        log.info(f"[{table}] CTAS thành công.")
 
    else:
        log.info(f"[{table}] Table đã tồn tại — INSERT INTO (append).")
 
        # Loại bỏ rows đã tồn tại theo business key (cross-batch dedup)
        bkeys = BUSINESS_KEYS[table]
        existing_keys = spark.table(iceberg_table).select(*bkeys)
        df_new = df.join(existing_keys, on=bkeys, how="left_anti")
 
        new_count = df_new.count()
        log.info(f"[{table}] Rows mới sau cross-batch dedup: {new_count}")
 
        if new_count > 0:
            df_new.writeTo(iceberg_table).append()
 
    log.info(f"[{table}] Ghi Bronze hoàn tất.")

def step_write_to_quarantine(df: DataFrame, table: str, batch_id: str, spark: SparkSession) -> None:
    """Ghi bad rows vào Bronze quarantine table."""
    if df.rdd.isEmpty():
        log.info(f"[{table}] Không có quarantine rows.")
        return
 
    quarantine_table = f"{ICEBERG_HIVE_CATALOG_NAME}.{QUARANTINE_DB}.{table}"
    quarantine_path  = f"{S3_BRONZE_QUARANTINE_BASE_PATH}/{table}"
 
    table_exists = spark.catalog.tableExists(quarantine_table)
 
    if not table_exists:
        df.createOrReplaceTempView(f"_tmp_quar_{table}")
        spark.sql(f"""
            CREATE TABLE {quarantine_table}
            USING iceberg
            LOCATION '{quarantine_path}'
            TBLPROPERTIES ('write.format.default' = 'parquet')
            AS SELECT * FROM _tmp_quar_{table}
        """)
    else:
        df.writeTo(quarantine_table).append()
 
    log.info(f"[{table}] Quarantine rows ghi xong: {quarantine_table}")

def step_tag_snapshot(spark: SparkSession, table: str, batch_id: str) -> None:
    """Gắn batch_id vào Iceberg snapshot summary để hỗ trợ Time Travel."""
    iceberg_table = f"{ICEBERG_HIVE_CATALOG_NAME}.{BRONZE_DB}.{table}"
    try:
        tag_snapshot(spark, iceberg_table, batch_id)
        log.info(f"[{table}] Snapshot tagged với batch_id={batch_id}")
    except Exception as e:
        # Non-fatal — không dừng pipeline nếu tag thất bại
        log.warning(f"[{table}] Không thể tag snapshot: {e}")

# ---------------------------------------------------------------------------
# Main entrypoint
# ---------------------------------------------------------------------------
 
def ingest_table(spark: SparkSession, table: str, batch_id: str) -> dict:
    """
    Orchestrate toàn bộ pipeline ingestion cho một bảng.
    Trả về dict metrics để Airflow có thể log.
    """
    log.info(f"========== START ingestion: {table} | batch_id={batch_id} ==========")
    source_file = CSV_FILES[table]
 
    # Step 1: Đọc CSV với schema enforcement
    raw_df = step_read_csv_stg(spark, table)
 
    # Step 2: Tách valid / quarantine
    valid_df, quarantine_df = step_split_valid_quarantine(raw_df, table)
 
    total_count     = raw_df.count()
    quarantine_count = quarantine_df.count()
    valid_count     = valid_df.count()
 
    # Step 3: Dedup trong batch
    deduped_df = dedup_by_business_key(valid_df, BUSINESS_KEYS[table])
    dedup_count = deduped_df.count()
 
    # Step 4: Thêm metadata + partition date column
    final_df = add_metadata_columns(deduped_df, batch_id, source_file)
    final_df = add_partition_date_col(final_df, PARTITION_COLS[table])
 
    # Thêm metadata cho quarantine rows
    quar_final_df = add_metadata_columns(quarantine_df, batch_id, source_file)
 
    # Step 5: Ghi vào Iceberg
    step_write_to_bronze(final_df, table, spark)
    step_write_to_quarantine(quar_final_df, table, batch_id, spark)
 
    # Step 6: Tag snapshot cho Time Travel
    step_tag_snapshot(spark, table, batch_id)
 
    metrics = {
        "table":              table,
        "batch_id":           batch_id,
        "total_raw_rows":     total_count,
        "valid_rows":         valid_count,
        "quarantine_rows":    quarantine_count,
        "written_rows":       dedup_count,
        "ingested_at":        datetime.now(timezone.utc).isoformat(),
    }
 
    log.info(f"[{table}] Metrics: {metrics}")
    log.info(f"========== END ingestion: {table} ==========")
    return metrics


def main():
    parser = argparse.ArgumentParser(description="Bronze ingestion PySpark job")
    parser.add_argument("--table",    required=True,
                        choices=list(SCHEMAS.keys()),
                        help="Tên bảng cần ingest")
    parser.add_argument("--batch_id", required=True,
                        help="Batch ID (do Airflow truyền vào)")
    args = parser.parse_args()
 
    spark = build_spark_session(f"bronze_ingestion_{args.table}")


    try:
        metrics = ingest_table(spark, args.table, args.batch_id)
        log.info(f"Job hoàn tất: {metrics}")
    except Exception as e:
        log.error(f"Job thất bại cho bảng {args.table}: {e}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()
 
 
if __name__ == "__main__":
    main()