#!/bin/bash
set -e

# Инициализация базы
airflow db init

# Создание админа
airflow users create \
  --username "${AIRFLOW_ADMIN_USER}" \
  --password "${AIRFLOW_ADMIN_PASSWORD}" \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email "${AIRFLOW_ADMIN_EMAIL}"

# Запуск webserver
exec airflow webserver
