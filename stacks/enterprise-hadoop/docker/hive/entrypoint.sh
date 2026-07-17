#!/usr/bin/env bash
# Hive 4.0.1 entrypoint with optional Ranger Hive plugin config
set -euo pipefail

HIVE_CONF_DIR="${HIVE_HOME:-/opt/hive}/conf"

# ── Ranger Hive plugin config (optional) ────────────────────────────────────

if [ "${RANGER_HIVE_PLUGIN_ENABLED:-false}" = "true" ]; then
  RANGER_CONF="${HIVE_CONF_DIR}/ranger-hive-security.xml"
  cat > "$RANGER_CONF" <<XML
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property><name>ranger.plugin.hive.policy.manager.url</name><value>${RANGER_ADMIN_URL:-http://ranger-admin:6080}</value></property>
  <property><name>ranger.plugin.hive.service.name</name><value>${RANGER_HIVE_SERVICE_NAME:-ehd-hive}</value></property>
  <property><name>ranger.plugin.hive.policy.cache.dir</name><value>/etc/ranger/hive/policycache</value></property>
  <property><name>ranger.plugin.hive.policy.pollIntervalMs</name><value>30000</value></property>
  <property><name>ranger.plugin.hive.policy.source.impl</name><value>org.apache.ranger.admin.client.RangerAdminRESTClient</value></property>
</configuration>
XML
  mkdir -p /etc/ranger/hive/policycache

  # Write Ranger Hive audit config
  cat > "${HIVE_CONF_DIR}/ranger-hive-audit.xml" <<XML
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property><name>xasecure.audit.is.enabled</name><value>true</value></property>
  <property><name>xasecure.audit.solr.is.enabled</name><value>true</value></property>
  <property><name>xasecure.audit.solr.solr_url</name><value>${RANGER_SOLR_URL:-http://solr:6983/solr/ranger_audits}</value></property>
</configuration>
XML
fi

# ── Ensure hive home exists (beeline needs ~/.beeline for history) ───────────
mkdir -p /home/hive/.beeline 2>/dev/null || true

# ── Remove stale PID file so container restarts don't false-detect "already running" ──
# Hive writes $$ (the bash PID, which is 1 in Docker) to hiveserver2.pid on start.
# After a container restart the file persists; kill -0 1 always succeeds, so Hive
# prints "HiveServer2 running as process 1. Stop it first." and exits in a crash loop.
_PID_DIR="${HIVESERVER2_PID_DIR:-${HIVE_CONF_DIR:-/opt/hive/conf}}"
rm -f "${_PID_DIR}/hiveserver2.pid" 2>/dev/null || true

# ── Delegate to official Hive entrypoint ────────────────────────────────────
# The official apache/hive image uses /entrypoint.sh
exec /entrypoint.sh "$@"
