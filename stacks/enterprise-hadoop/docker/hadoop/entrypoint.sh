#!/usr/bin/env bash
# Hadoop 3.4.1 container entrypoint
# Converts CORE_CONF_*, HDFS_CONF_*, YARN_CONF_*, MAPRED_CONF_* env vars to XML
# then dispatches to the correct Hadoop role via HADOOP_ROLE env var.
#
# Key naming: PREFIX_key___sub___key  (triple ___ = hyphen, single _ = dot)
# Matches bde2020 hadoop image convention so existing hadoop.env works unchanged.
set -euo pipefail

HADOOP_CONF_DIR="${HADOOP_HOME}/etc/hadoop"

# ── env-to-XML converter ─────────────────────────────────────────────────────

add_property() {
  local file=$1 name=$2
  # Shift so $@ is the value (handles spaces/special chars)
  shift 2
  local value="$*"
  local entry
  entry="  <property><name>${name}</name><value>${value}</value></property>"
  sed -i "s|</configuration>|${entry}\n</configuration>|" "$file"
}

configure() {
  local file=$1 prefix=$2

  cat > "$file" <<'XML'
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
</configuration>
XML

  while IFS='=' read -r name value; do
    # Strip prefix (e.g. CORE_CONF_)
    local key="${name#${prefix}_}"
    # ___ -> hyphen placeholder, __ -> underscore placeholder, _ -> dot
    key="${key//___/$'\x01'}"
    key="${key//__/$'\x02'}"
    key="${key//_/.}"
    key="${key//$'\x01'/-}"
    key="${key//$'\x02'/_}"
    add_property "$file" "$key" "$value"
  done < <(env | grep "^${prefix}_" | sort)
}

configure "${HADOOP_CONF_DIR}/core-site.xml"   CORE_CONF
configure "${HADOOP_CONF_DIR}/hdfs-site.xml"   HDFS_CONF
configure "${HADOOP_CONF_DIR}/yarn-site.xml"   YARN_CONF
configure "${HADOOP_CONF_DIR}/mapred-site.xml" MAPRED_CONF

# ── Ranger HDFS plugin config (optional) ────────────────────────────────────

if [ "${RANGER_HDFS_PLUGIN_ENABLED:-false}" = "true" ]; then
  RANGER_CONF="${HADOOP_CONF_DIR}/ranger-hdfs-plugin.xml"
  cat > "$RANGER_CONF" <<XML
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property><name>ranger.plugin.hdfs.policy.manager.url</name><value>${RANGER_ADMIN_URL:-http://ranger-admin:6080}</value></property>
  <property><name>ranger.plugin.hdfs.service.name</name><value>${RANGER_HDFS_SERVICE_NAME:-ehd-hdfs}</value></property>
  <property><name>ranger.plugin.hdfs.policy.cache.dir</name><value>/etc/ranger/hdfs/policycache</value></property>
  <property><name>ranger.plugin.hdfs.policy.pollIntervalMs</name><value>30000</value></property>
  <property><name>ranger.plugin.hdfs.policy.source.impl</name><value>org.apache.ranger.admin.client.RangerAdminRESTClient</value></property>
</configuration>
XML
  mkdir -p /etc/ranger/hdfs/policycache

  # Enable Ranger authorizer in hdfs-site.xml
  add_property "${HADOOP_CONF_DIR}/hdfs-site.xml" \
    "dfs.namenode.inode.attributes.provider.class" \
    "org.apache.ranger.authorization.hadoop.RangerHdfsAuthorizer"
fi

# ── Service dispatcher ────────────────────────────────────────────────────────

case "${HADOOP_ROLE}" in
  namenode)
    NAMENODE_DATA_PATH=$(echo "${HDFS_CONF_dfs_namenode_name_dir:-file:///hadoop/dfs/name}" | sed 's|^file://||')
    if [ ! -d "${NAMENODE_DATA_PATH}/current" ]; then
      echo "[namenode] Formatting HDFS..."
      hdfs namenode -format -nonInteractive -force
    fi
    exec hdfs namenode
    ;;
  datanode)
    exec hdfs datanode
    ;;
  resourcemanager)
    exec yarn resourcemanager
    ;;
  nodemanager)
    exec yarn nodemanager
    ;;
  historyserver)
    exec mapred historyserver
    ;;
  *)
    echo "ERROR: HADOOP_ROLE='${HADOOP_ROLE:-}' is not set or unknown."
    echo "  Set HADOOP_ROLE to one of: namenode datanode resourcemanager nodemanager historyserver"
    exit 1
    ;;
esac
