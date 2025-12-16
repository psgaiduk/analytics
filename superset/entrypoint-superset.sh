#!/bin/bash
set -e

superset db upgrade

superset fab create-admin \
  --username "${SUPERSET_ADMIN_USER}" \
  --password "${SUPERSET_ADMIN_PASSWORD}" \
  --firstname Admin \
  --lastname User \
  --email "${SUPERSET_ADMIN_EMAIL}" || true

superset init

exec superset run \
  -h 0.0.0.0 \
  -p 8088 \
  --with-threads \
  --reload
