from datetime import datetime, timezone
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType
)

from olist_retail_lakehouse.spark_jobs.init_setup.setup_const import PIPELINE_VERSION

__all__ = []


def add_metadata_columns(df: DataFrame, batch_id: str, source_file: str) -> DataFrame:
    """Thêm các metadata columns chuẩn cho Bronze layer."""
    return (
        df
        .withColumn("_ingested_at",      F.current_timestamp())
        .withColumn("_source_file",      F.lit(source_file))
        .withColumn("_batch_id",         F.lit(batch_id))
        .withColumn("_pipeline_version", F.lit(PIPELINE_VERSION))
    )

def build_null_check_condition(not_null_cols: list[str]):
    """Build condition: TRUE nếu bất kỳ cột NOT NULL nào là null."""
    condition = F.lit(False)
    for col in not_null_cols:
        condition = condition | F.col(col).isNull()
    return condition
 
 
def build_reject_reason(not_null_cols: list[str]):
    """Build CASE WHEN để tạo reject_reason string."""
    expr = F.lit("")
    for col in not_null_cols:
        expr = F.when(
            F.col(col).isNull(),
            F.concat_ws("; ", expr, F.lit(f"{col} IS NULL"))
        ).otherwise(expr)
    return F.trim(F.regexp_replace(expr, "^; ", ""))


def spark_read_csv_path(spark: SparkSession, csv_path: str, schema: StructType) -> DataFrame:
    df = (
        spark.read
        .option("header", "true")
        .option("encoding", "UTF-8")
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", "true")
        .option("delimiter", ",")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .option("dateFormat",      "M/d/yyyy H:mm")        # format Olist review_creation_date
        .option("mode", "PERMISSIVE")   # không drop row lỗi parse — xử lý thủ công bên dưới
        .schema(schema)
        .csv(csv_path)
    )
    return df

def dedup_by_business_key(df: DataFrame, bkeys: list[str]) -> DataFrame:
    """
    Dedup trong batch hiện tại dùng business key.
    Giữ row đầu tiên trong file (row_number = 1).
    Dedup cross-batch sẽ được xử lý ở Silver bằng ROW_NUMBER + _ingested_at.
    """
    window = (
        Window.partitionBy(*bkeys)
        .orderBy(F.monotonically_increasing_id())
    )
    return (
        df.withColumn("_rn", F.row_number().over(window))
          .filter(F.col("_rn") == 1)
          .drop("_rn")
    )

def add_partition_date_col(df: DataFrame, ts_col: str) -> DataFrame:
    """Thêm DATE column từ timestamp để dùng làm Iceberg partition."""
    if ts_col and ts_col in df.columns:
        df = df.withColumn(f"{ts_col}_date", F.to_date(F.col(ts_col)))
    return df

def tag_snapshot(spark: SparkSession, iceberg_table: str, batch_id: str) -> None:
    """Gắn batch_id vào Iceberg snapshot summary để hỗ trợ Time Travel."""
    spark.sql(f"""
        ALTER TABLE {iceberg_table}
        SET TBLPROPERTIES (
            'snapshot.summary.batch_id'         = '{batch_id}',
            'snapshot.summary.pipeline_version' = '{PIPELINE_VERSION}'
        )
    """)