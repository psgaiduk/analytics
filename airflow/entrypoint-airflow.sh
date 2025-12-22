#!/bin/bash
set -e

# -------------------------------
# Simple Auth Manager (Airflow 3.x)
# -------------------------------
PASSWORD_FILE="/opt/airflow/simple_auth_data/simple_auth_manager_passwords.json"
mkdir -p "$(dirname "$PASSWORD_FILE")"

if [ ! -f "$PASSWORD_FILE" ]; then
    echo "{\"admin\": \"${AIRFLOW_ADMIN_PASSWORD}\"}" > "$PASSWORD_FILE"
    echo "Создан файл с паролем admin"
else
    echo "Файл с паролем уже существует, пропускаем создание"
fi

# -------------------------------
# Миграция базы (один раз безопасно)
# -------------------------------
airflow db migrate

# -------------------------------
# Запуск нужного сервиса
# -------------------------------
exec airflow standalone
