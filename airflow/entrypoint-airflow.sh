#!/bin/bash
set -e

airflow db migrate

# Создаём admin пользователя ТОЛЬКО для webserver
if [ "$1" = "webserver" ]; then
  echo "Checking if admin user exists..."

  if ! airflow users list | grep -q "^${AIRFLOW_ADMIN_USER}\b"; then
    echo "Creating admin user..."
    airflow users create \
      --username "${AIRFLOW_ADMIN_USER}" \
      --password "${AIRFLOW_ADMIN_PASSWORD}" \
      --firstname Admin \
      --lastname User \
      --role Admin \
      --email "${AIRFLOW_ADMIN_EMAIL}"
  else
    echo "Admin user already exists"
  fi
fi

case "$1" in
  webserver)
    exec airflow webserver
    ;;
  scheduler)
    exec airflow scheduler
    ;;
  *)
    exec airflow "$@"
    ;;
esac
