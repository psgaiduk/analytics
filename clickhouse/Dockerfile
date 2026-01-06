FROM clickhouse/clickhouse-server:25.8.12.129

COPY init /docker-entrypoint-initdb.d

RUN chown -R clickhouse:clickhouse /docker-entrypoint-initdb.d && \
    chmod -R 755 /docker-entrypoint-initdb.d