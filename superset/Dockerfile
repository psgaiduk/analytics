FROM apache/superset:4.1.4

ENV SUPERSET_ADMIN_USER=${SUPERSET_ADMIN_USER}
ENV SUPERSET_ADMIN_PASSWORD=${SUPERSET_ADMIN_PASSWORD}
ENV SUPERSET_ADMIN_EMAIL=${SUPERSET_ADMIN_EMAIL}
ENV SUPERSET_SECRET_KEY=${SUPERSET_SECRET_KEY}

COPY superset_config.py /app/pythonpath/superset_config.py
ENV SUPERSET_CONFIG_PATH=/app/pythonpath/superset_config.py

COPY entrypoint-superset.sh /entrypoint-superset.sh
ENTRYPOINT ["bash", "/entrypoint-superset.sh"]
