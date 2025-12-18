#!/bin/bash
set -e

# --- Генерация пароля admin для Simple Auth Manager ---
PASSWORD_FILE="/opt/airflow/simple_auth_data/simple_auth_manager_passwords.json"
mkdir -p "$(dirname "$PASSWORD_FILE")"

if [ ! -f "$PASSWORD_FILE" ]; then
    echo "{\"admin\": \"${AIRFLOW_ADMIN_PASSWORD}\"}" > "$PASSWORD_FILE"
    echo "Создан файл с паролем admin"
else
    echo "Файл с паролем уже существует, пропускаем создание"
fi

# --- Миграция базы (при старте контейнера) ---
airflow db migrate

# --- Запуск нужного сервиса ---
case "$1" in
    api-server)
        exec airflow api-server
        ;;
    scheduler)
        exec airflow scheduler
        ;;
    *)
        exec airflow "$@"
        ;;
esac
