#!/bin/bash
set -e

export MB_PLUGINS_DIR=/plugins
export MB_DB_FILE=/metabase-data/metabase.db
export MB_JETTY_HOST=0.0.0.0
export MB_JETTY_PORT=3000

/app/run_metabase.sh &

echo "Waiting for Metabase to start..."
until curl -s http://localhost:3000/api/health | grep -q "ok"; do
    printf '.'
    sleep 5
done

echo -e "\nMetabase is UP."

echo "Step 1: Admin creation..."
curl -s -X POST http://localhost:3000/api/setup \
  -H "Content-Type: application/json" \
  -d "{
    \"token\": \"$MB_SETUP_TOKEN\",
    \"user\": {
        \"first_name\": \"$MB_ADMIN_FIRST_NAME\",
        \"last_name\": \"$MB_ADMIN_LAST_NAME\",
        \"email\": \"$MB_ADMIN_EMAIL\",
        \"password\": \"$MB_ADMIN_PASSWORD\"
    },
    \"database\": null,
    \"prefs\": { \"site_name\": \"My Analytics BI\", \"allow_tracking\": false }
}" > /dev/null 2>&1 || echo "Admin already exists."

echo "Step 2: Connecting ClickHouse..."

SESSION_JSON=$(curl -s -X POST http://localhost:3000/api/session \
  -H "Content-Type: application/json" \
  -d "{\"username\": \"$MB_ADMIN_EMAIL\", \"password\": \"$MB_ADMIN_PASSWORD\"}")

SESSION_TOKEN=$(echo "$SESSION_JSON" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')

if [ -n "$SESSION_TOKEN" ] && [ ${#SESSION_TOKEN} -gt 10 ]; then
    echo "Login successful. Checking databases..."
    
    DB_LIST=$(curl -s -H "X-Metabase-Session: $SESSION_TOKEN" http://localhost:3000/api/database)

    if echo "$DB_LIST" | grep -q "ClickHouse Main"; then
        echo "DATABASE ALREADY EXISTS: ClickHouse Main is connected."
    else
        echo "DATABASE NOT FOUND: Adding ClickHouse now..."
        ADD_DB=$(curl -s -X POST http://localhost:3000/api/database \
          -H "Content-Type: application/json" \
          -H "X-Metabase-Session: $SESSION_TOKEN" \
          -d "{
            \"name\": \"ClickHouse Main\",
            \"engine\": \"clickhouse\",
            \"details\": {
                \"host\": \"${CLICKHOUSE_HOST}\",
                \"port\": ${CLICKHOUSE_PORT},
                \"db\": \"${CLICKHOUSE_DB_WORK}\",
                \"user\": \"${CLICKHOUSE_USER}\",
                \"password\": \"${CLICKHOUSE_PASSWORD}\",
                \"ssl\": false
            }
          }")
        
        if echo "$ADD_DB" | grep -q "id"; then
            echo "SUCCESS: ClickHouse database added!"
        else
            echo "ERROR: Failed to add DB. Response: $ADD_DB"
        fi
    fi
else
    echo "ERROR: Could not get session token. API Response: $SESSION_JSON"
fi

echo "All configuration steps finished."
wait