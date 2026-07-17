#!/usr/bin/env bash
# Ranger Usersync 2.4.0 entrypoint
set -euo pipefail

USERSYNC_HOME=/opt/ranger/usersync
SETUP_DONE=/opt/ranger/usersync/.setup_done

RANGER_ADMIN_URL="${RANGER_ADMIN_URL:-http://ranger-admin:6080}"
USERSYNC_PASSWORD="${USERSYNC_PASSWORD:-rangerusersync123}"
SYNC_SOURCE="${SYNC_SOURCE:-unix}"

wait_tcp() {
  local host=$1 port=$2 name=$3
  echo "[ranger-usersync] waiting for ${name} (${host}:${port})..."
  until nc -z "${host}" "${port}" 2>/dev/null; do sleep 3; done
  echo "[ranger-usersync] ${name} ready"
}

# Wait for Ranger Admin to be up
RANGER_HOST=$(echo "$RANGER_ADMIN_URL" | sed 's|.*://||' | cut -d: -f1)
RANGER_PORT=$(echo "$RANGER_ADMIN_URL" | sed 's|.*://||' | cut -d: -f2 | tr -d '/')
wait_tcp "${RANGER_HOST}" "${RANGER_PORT:-6080}" "Ranger Admin"

# Also wait for admin HTTP to be ready
echo "[ranger-usersync] waiting for Ranger Admin HTTP..."
until curl -sf "${RANGER_ADMIN_URL}/index.html" >/dev/null 2>&1; do sleep 5; done

if [ ! -f "$SETUP_DONE" ]; then
  cd "$USERSYNC_HOME"
  cp install.properties install.properties.orig 2>/dev/null || true

  sed -i "s|^POLICY_MGR_URL=.*|POLICY_MGR_URL=${RANGER_ADMIN_URL}|"         install.properties
  sed -i "s|^SYNC_SOURCE=.*|SYNC_SOURCE=${SYNC_SOURCE}|"                    install.properties
  sed -i "s|^rangerUsersync_password=.*|rangerUsersync_password=${USERSYNC_PASSWORD}|" install.properties

  echo "[ranger-usersync] running setup.sh..."
  export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
  bash setup.sh

  touch "$SETUP_DONE"
  echo "[ranger-usersync] setup complete"
fi

# Start Usersync
cd "$USERSYNC_HOME"
echo "[ranger-usersync] starting Ranger Usersync..."
./ranger-usersync start || true

exec tail -F "${USERSYNC_HOME}/logs/usersync.log" 2>/dev/null || \
  exec sleep infinity
