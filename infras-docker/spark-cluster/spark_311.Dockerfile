# ---------- Base Layer ----------
FROM python:3.11.14-slim AS spark-base

SHELL ["/bin/bash", "-euxo", "pipefail", "-c"]

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        sudo \
        curl \
        vim \
        unzip \
        procps \
        # rsync \
        # ssh \
        openjdk-21-jdk \
        build-essential && \
        # software-properties-common && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV SPARK_VERSION=3.5.1 \
    HADOOP_VERSION=hadoop3 \
    SPARK_HOME=/opt/spark \
    HADOOP_HOME=/opt/hadoop \
    JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

RUN mkdir -p "${SPARK_HOME}" "${HADOOP_HOME}"
WORKDIR ${SPARK_HOME}

# RUN curl -fsSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-${HADOOP_VERSION}.tgz" -o spark.tgz && \
#     tar -xvzf spark.tgz --strip-components=1 && \
#     rm -f spark.tgz

COPY spark-${SPARK_VERSION}-bin-${HADOOP_VERSION}.tgz /tmp/spark.tgz
RUN tar -xvzf /tmp/spark.tgz --strip-components=1 && \
    rm -f /tmp/spark.tgz
# ---------- Runtime Layer ----------
FROM spark-base AS pyspark

ENV PATH="/opt/spark/sbin:/opt/spark/bin:${PATH}" \
    SPARK_HOME="/opt/spark" \
    SPARK_MASTER="spark://spark-master:7077" \
    SPARK_MASTER_HOST="spark-master" \
    SPARK_LOG_DIR="/opt/spark/logs" \
    SPARK_MASTER_LOG="/opt/spark/logs/spark-master.out" \
    SPARK_WORKER_LOG="/opt/spark/logs/spark-worker.out" \
    SPARK_MASTER_PORT=7077 \
    PYSPARK_PYTHON="python3" \
    JAVA_HOME="/usr/lib/jvm/java-21-openjdk-amd64"

RUN chmod +x /opt/spark/sbin/* /opt/spark/bin/* && \
    mkdir -p $SPARK_LOG_DIR && \
    touch $SPARK_MASTER_LOG && \
    touch $SPARK_WORKER_LOG && \
    ln -sf /dev/stdout $SPARK_MASTER_LOG && \
    ln -sf /dev/stdout $SPARK_WORKER_LOG

# COPY conf/spark-defaults.conf "$SPARK_HOME/conf"
# COPY requirements/requirements.txt .
# RUN pip install --no-cache-dir -r requirements.txt

# COPY entrypoint.sh /opt/entrypoint.sh
# RUN chmod +x /opt/entrypoint.sh

# ENTRYPOINT ["/opt/entrypoint.sh"]

COPY start-spark.sh /
RUN chmod +x /start-spark.sh
CMD ["/bin/bash", "/start-spark.sh"]

