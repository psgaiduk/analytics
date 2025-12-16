#!/bin/bash
set -e

superset db upgrade

superset fab create-admin \
  --username "${SUPERSET_ADMIN_USER}" \
  --password "${SUPERSET_ADMIN_PASSWORD}" \
  --firstname Admin \
  --lastname User \
  --email "${SUPERSET_ADMIN_EMAIL}"

superset init

exec superset run -p 8088 --with-threads --reload
