import clickhouse_connect
import os

client = clickhouse_connect.get_client(
    host=os.getenv('CLICKHOUSE_HOST', 'clickhouse'),
    port=int(os.getenv('CLICKHOUSE_PORT', 8123)),
    username=os.getenv('CLICKHOUSE_USER', 'default'),
    password=os.getenv('CLICKHOUSE_PASSWORD', '')
)

# # Проверка подключения
# result = client.query('SELECT version()')
# print(f"Connected to ClickHouse version: {result.first_row[0]}")