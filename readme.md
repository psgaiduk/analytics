# Сервис для аналитики данных

Сквозная аналитическая платформа для сбора, хранения и визуализации результатов. Сейчас данные есть по биатлону с сайта biathlonresults.com.

---

## Стек технологий
* **Orchestration:** Airflow (запуск DAG-ов, ELT процессы).
* **Storage:** ClickHouse (аналитическая БД), Postgres (метаданные Airflow).
* **BI Tools:** Metabase & Apache Superset.
* **Reporting:** Quarto: Статическая отчетность, публикации и PDF-дайджесты на Python.
* **Transformation:** dbt (подготовка витрин данных).
* **Infrastructure:** Docker Compose.

---

## Потоки данных (Data Pipeline)

1. **Extract**: Airflow забирает данные с `biathlonresults.com`.
2. **Load**: Данные проходят дедупликацию и загружаются в ClickHouse.
3. **Transform**: dbt трансформирует «сырые» данные в аналитические витрины.
4. **Visualize**: 

    4.1. Metabase и Superset подключаются к ClickHouse для построения дашбордов.
    4.2. Notebooks: Jupyter/Quarto для создания аналитических  книг


---

## Быстрый старт

**1. Создать файл переменных окружения**:

```bash
AIRFLOW_DB_USER=airflow
AIRFLOW_DB_PASSWORD=airflow
AIRFLOW_DB_NAME=airflow
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=admin
AIRFLOW_ADMIN_EMAIL=admin@example.com
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__CORE__FERNET_KEY=l4RVgWqCOOpOBGIJZeRYl6lujfkd8LuPvWqnIXaaPsM=
AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS=admin:admin
AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE=/opt/airflow/simple_auth_data/simple_auth_manager_passwords.json
AIRFLOW__API__BASE_URL=http://localhost:8080

# Postgres
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow

# Superset
SUPERSET_ADMIN_USER=admin
SUPERSET_ADMIN_PASSWORD=admin
SUPERSET_ADMIN_EMAIL=admin@example.com
SUPERSET_SECRET_KEY=biathlon_secret

# ClickHouse
CLICKHOUSE_DB=biathlon_raw
CLICKHOUSE_DB_WORK=biathlon
CLICKHOUSE_HOST=clickhouse
CLICKHOUSE_PORT=8123
CLICKHOUSE_USER=admin
CLICKHOUSE_PASSWORD=admin

# Metabase
MB_SETUP_TOKEN=some-very-long-secret-token-123
MB_ADMIN_EMAIL=admin@example.com
MB_ADMIN_PASSWORD=1111admin0000
MB_ADMIN_FIRST_NAME=Admin
MB_ADMIN_LAST_NAME=User

# Quarto
JUPYTER_PASSWORD_HASH='argon2:$argon2id$v=19$m=10240,t=10,p=8$4HKnkL3sdr4aCDD3ECpUtg$H2zyIx1tg+oeybobiGvC7DVtrYGU/XezsMo21BJxCkM'
```

! Заменить все логины и пароли

**2. Собрать сервисы**:

```docker-compose up -d --build```

### Доступы к сервисам

| Сервис | URL |
| :--- | :--- |
| **Airflow** | [http://localhost:8080](http://localhost:8080) 
| **Metabase** | [http://localhost:3000](http://localhost:3000) 
| **Superset** | [http://localhost:8088](http://localhost:8088)
| **Quarto** | [http://localhost:8888](http://localhost:8888) 

Логины и пароли берутся из файла .env

## Dags в Airflow

### dbt_run	
Запуск моделей dbt для пересчета аналитических витрин в ClickHouse. Предварительно удаляет все старые таблицы из базы данных.

### biathlon_season_results
Ежедневное обновление результатов текущего сезона. Получает все события на текущий сезон и результаты завершённых гонок.

### biathlon_update_oldest_season

Ежедневное обновляет данные самого старого сезона. Получает все события на текущий сезон и результаты завершённых гонок.

### biathlon_update_all_seasons	
Запускается руками, обновляет все данные по всем гонкам за всю историю турнира. Работает около суток.

### biathlon_update_race_results
Используется для ручного перезапуска сбора данных по конкретной гонке (race_id) через параметры.