import os

# ===== Variables =====

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")

S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")

THRIFT_HIVE_URI = os.getenv("THRIFT_HIVE_URI")
ICEBERG_HIVE_CATALOG_NAME = os.getenv("ICEBERG_HIVE_CATALOG_NAME")


# ===== Logging format =====
LOGGING_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


PIPELINE_VERSION = "v1.0.0"  # Cố định version để dễ tracking, có thể override bằng env var nếu cần
