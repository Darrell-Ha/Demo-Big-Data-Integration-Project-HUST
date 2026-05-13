#!/bin/bash

export HIVE_HOME=/opt/hive
export HIVE_CONF_DIR=${HIVE_HOME}/conf
export HIVE_AUX_JARS_PATH=${HIVE_HOME}/auxlib
export HIVE_CUSTOM_CONF_DIR=${HADOOP_HOME}/conf
export PATH=$PATH:$HIVE_HOME/bin

DB_HOST=metastore-pg.retail-lake.domain
DB_PORT=5432
DB_NAME=hive_metastore
DB_USER=hive
DB_PASSWORD=hive

export PGPASSWORD=$DB_PASSWORD

echo "Checking Hive metastore schema..."

SCHEMA_EXISTS=$(psql \
  -h $DB_HOST \
  -p $DB_PORT \
  -U $DB_USER \
  -d $DB_NAME \
  -tAc "SELECT 1 FROM information_schema.tables WHERE table_name='VERSION';")

if [ "$SCHEMA_EXISTS" = "1" ]; then
  echo "Hive metastore schema already initialized. Skipping initSchema."
else
  echo "Hive metastore schema not found. Initializing..."
  schematool -dbType postgres -initSchema
fi

echo "Starting Hive Metastore..."
exec hive --service metastore

