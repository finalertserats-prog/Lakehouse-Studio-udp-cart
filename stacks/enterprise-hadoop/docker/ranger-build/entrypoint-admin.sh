#!/usr/bin/env bash
# Ranger Admin 2.4.0 entrypoint
# On first start: waits for Postgres + Solr, runs setup.sh, starts admin.
# On subsequent starts: skips setup, starts admin directly.
set -euo pipefail

RANGER_HOME=/opt/ranger/admin
SETUP_DONE=/opt/ranger/admin/.setup_done

DB_HOST="${DB_HOST:-postgres}"
DB_PORT="${DB_PORT:-5432}"
DB_NAME="${DB_NAME:-ranger}"
DB_ROOT_USER="${DB_ROOT_USER:-postgres}"
DB_ROOT_PASSWORD="${DB_ROOT_PASSWORD:-postgres}"
DB_USER="${DB_USER:-rangeradmin}"
DB_PASSWORD="${DB_PASSWORD:-rangeradmin123}"
RANGER_ADMIN_PASSWORD="${RANGER_ADMIN_PASSWORD:-rangeradmin123}"
SOLR_HOST="${SOLR_HOST:-solr}"
SOLR_PORT="${SOLR_PORT:-6983}"
AUDIT_SOLR_URL="${AUDIT_SOLR_URL:-http://${SOLR_HOST}:${SOLR_PORT}/solr/ranger_audits}"
RANGER_HTTP_PORT="${RANGER_HTTP_PORT:-6080}"

wait_tcp() {
  local host=$1 port=$2 name=$3
  echo "[ranger-admin] waiting for ${name} (${host}:${port})..."
  until nc -z "${host}" "${port}" 2>/dev/null; do sleep 3; done
  echo "[ranger-admin] ${name} ready"
}

if [ ! -f "$SETUP_DONE" ]; then
  wait_tcp "${DB_HOST}" "${DB_PORT}" "PostgreSQL"
  wait_tcp "${SOLR_HOST}" "${SOLR_PORT}" "Solr"

  # Configure install.properties from env vars
  cd "$RANGER_HOME"
  cp install.properties install.properties.orig 2>/dev/null || true

  sed -i "s|^DB_FLAVOR=.*|DB_FLAVOR=POSTGRES|"                             install.properties
  sed -i "s|^SQL_CONNECTOR_JAR=.*|SQL_CONNECTOR_JAR=${RANGER_HOME}/ews/lib/postgresql-42.7.3.jar|" install.properties
  sed -i "s|^db_root_user=.*|db_root_user=${DB_ROOT_USER}|"                install.properties
  sed -i "s|^db_root_password=.*|db_root_password=${DB_ROOT_PASSWORD}|"    install.properties
  sed -i "s|^db_host=.*|db_host=${DB_HOST}|"                               install.properties
  sed -i "s|^db_name=.*|db_name=${DB_NAME}|"                               install.properties
  sed -i "s|^db_user=.*|db_user=${DB_USER}|"                               install.properties
  sed -i "s|^db_password=.*|db_password=${DB_PASSWORD}|"                   install.properties
  sed -i "s|^rangeradmin_password=.*|rangeradmin_password=${RANGER_ADMIN_PASSWORD}|" install.properties
  sed -i "s|^audit_store=.*|audit_store=solr|"                             install.properties
  sed -i "s|^audit_solr_url=.*|audit_solr_url=${AUDIT_SOLR_URL}|"          install.properties
  sed -i "s|^ranger_admin_http_port=.*|ranger_admin_http_port=${RANGER_HTTP_PORT}|" install.properties || \
    echo "ranger_admin_http_port=${RANGER_HTTP_PORT}" >> install.properties

  echo "[ranger-admin] running setup.sh..."
  export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
  bash setup.sh

  touch "$SETUP_DONE"
  echo "[ranger-admin] setup complete"
fi

# Start Ranger Admin
cd "$RANGER_HOME"
echo "[ranger-admin] starting Ranger Admin on port ${RANGER_HTTP_PORT}..."
./ranger-admin start || true

# Wait for Ranger Admin to respond
echo "[ranger-admin] waiting for web server..."
for i in $(seq 1 60); do
  if curl -sf "http://localhost:${RANGER_HTTP_PORT}/index.html" >/dev/null 2>&1; then
    echo "[ranger-admin] Ranger Admin is up at http://localhost:${RANGER_HTTP_PORT}"
    break
  fi
  sleep 5
done

# Keep container alive and stream logs
exec tail -F "${RANGER_HOME}/ews/logs/ranger-admin-app.log" 2>/dev/null || \
  exec tail -F "${RANGER_HOME}/ews/logs/ranger-admin.log"   2>/dev/null || \
  exec sleep infinity
