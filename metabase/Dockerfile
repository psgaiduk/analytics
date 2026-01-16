FROM metabase/metabase:v0.58.2.x

USER root

RUN apk update && apk add --no-cache curl bash

RUN mkdir -p /plugins /metabase-data && chmod -R 777 /plugins /metabase-data

RUN curl -L https://github.com/metabase/clickhouse-driver/releases/download/1.5.1/clickhouse.metabase-driver.jar \
    -o /plugins/clickhouse.metabase-driver.jar && \
    chmod 644 /plugins/clickhouse.metabase-driver.jar

COPY entrypoint-metabase.sh /entrypoint-metabase.sh
RUN chmod +x /entrypoint-metabase.sh

WORKDIR /metabase-data

USER 2000

ENTRYPOINT ["/bin/bash", "/entrypoint-metabase.sh"]