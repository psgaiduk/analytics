#!/bin/bash
set -e

# Миграция базы (при старте контейнера)
airflow db migrate

# Запуск нужного сервиса
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
