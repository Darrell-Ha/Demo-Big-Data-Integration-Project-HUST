FROM apache/airflow:2.10.4-python3.11

# Cài đặt bổ sung một số thư viện hệ thống
USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    net-tools \
    krb5-user \
    libkrb5-dev \
    gcc \
    # Cài đặt java 8
    && mkdir -p /etc/apt/keyrings \
    && curl -fsSL https://packages.adoptium.net/artifactory/api/gpg/key/public | gpg --dearmor -o /etc/apt/keyrings/adoptium.gpg \
    && echo "deb [signed-by=/etc/apt/keyrings/adoptium.gpg] https://packages.adoptium.net/artifactory/deb bookworm main" > /etc/apt/sources.list.d/adoptium.list \
    && apt-get update \
    && apt-get install -y temurin-8-jre \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
    
USER airflow

# Thiết lập môi trường
ENV AIRFLOW_HOME=/opt/airflow \
    JAVA_HOME=/usr/lib/jvm/temurin-8-jre-amd64

# Cài đặt các thư viện bổ sung
RUN uv pip install --no-cache-dir \
    apache-airflow[apache-hdfs,apache-webhdfs,apache-spark,apache-iceberg,trino]==2.10.4 \
    pyspark==3.5.1 \
    pandas numpy requests openpyxl pyarrow faker \
    dbt-adapters==1.16.7 dbt-common==1.37.2 dbt-core==1.11.2 dbt-trino==1.10.0
