#!/bin/bash

# Load env vars from .env
if [ -f /opt/airflow/.env ]; then
    echo "Loading environment variables from /opt/airflow/.env"
    set -a  # Tự động export các biến
    source /opt/airflow/.env
    set +a
else
    echo "Warning: /opt/airflow/.env not found, relying on environment variables"
fi

# Setting env for airflow home
export AIRFLOW_HOME=${AIRFLOW_HOME:-/opt/airflow}

# Check necessary env vars
: "${AIRFLOW__DATABASE__SQL_ALCHEMY_CONN?Need to set AIRFLOW__DATABASE__SQL_ALCHEMY_CONN}"
: "${AIRFLOW__CORE__EXECUTOR?Need to set AIRFLOW__CORE__EXECUTOR}"

if [ "$AIRFLOW__CORE__EXECUTOR" = "CeleryExecutor" ]; then
    : "${AIRFLOW__CELERY__BROKER_URL?Need to set AIRFLOW__CELERY__BROKER_URL}"
fi

# Initialize airflow metadata database
airflow db migrate

airflow connections delete spark_cluster || true

airflow connections add spark_cluster \
  --conn-type spark \
  --conn-host 'spark://spark-master:7077' \
  --conn-extra '{"deploy-mode": "client"}'

# Create admin user for airflow
airflow users create \
    --username "${AIRFLOW_ADMIN_USERNAME:-admin}" \
    --firstname "${AIRFLOW_ADMIN_FIRSTNAME:-Admin}" \
    --lastname "${AIRFLOW_ADMIN_LASTNAME:-User}" \
    --role Admin \
    --email "${AIRFLOW_ADMIN_EMAIL:-admin@example.com}" \
    --password "${AIRFLOW_ADMIN_PASSWORD:-admin}" || true

# Execute
exec airflow "$@"