"""ai_configurator.py — LLM-driven config file generator for custom stacks.

Given a resolved component list + version overrides, calls the LLM to produce
every config file the stack needs:
  - config/spark/spark-defaults.conf
  - config/trino/catalog/<name>.properties
  - config/hadoop/core-site.xml  (when HDFS is present)
  - config/hadoop/hdfs-site.xml  (when HDFS is present)
  - hive-metastore-site.xml
  - config/hive/hive-site.xml    (when Hive HMS is present)
  - config/flink/flink-conf.yaml (when Flink is present)
  - nessie.properties
  - config/prometheus/prometheus.yml
  - config/loki/loki.yml
  - config/grafana/datasources/datasources.yaml
  - config/dagster/workspace.yaml (when Dagster is present)
  - config/dbt/profiles.yml       (when dbt is present)
  - .env  (all credentials)
  - pipeline_example.py  (starter PySpark code)
  - connection_info dict  (endpoints for the UI summary)
  - post_start_commands list  (init steps to run after docker compose up)
  - connectivity_checks list  (commands to verify inter-service wiring)
"""
from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any

import litellm

from .component_registry import COMPONENTS


# ---------------------------------------------------------------------------
# LLM helpers
# ---------------------------------------------------------------------------

def _call_llm(prompt: str) -> dict:
    resp = litellm.completion(
        model=os.environ.get("LITELLM_MODEL", "gpt-4o-mini"),
        api_base=os.environ.get("LITELLM_BASE_URL"),
        api_key=os.environ.get("LITELLM_API_KEY"),
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2,
        max_tokens=8192,
    )
    raw: str = resp.choices[0].message.content.strip()

    # Strip markdown fences
    if raw.startswith("```"):
        raw = re.sub(r"^```[a-z]*\n?", "", raw)
        raw = re.sub(r"\n?```$", "", raw.rstrip())

    # Fix triple double-quotes that break JSON
    raw = raw.replace('"""', "'''")
    # Remove trailing commas before } or ]
    raw = re.sub(r",\s*([}\]])", r"\1", raw)

    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return _fallback_extract(raw)


def _fallback_extract(raw: str) -> dict:
    """Best-effort regex extraction when full JSON parse fails."""
    result: dict = {}
    string_fields = (
        "spark_defaults",
        "core_site_xml", "hdfs_site_xml", "yarn_site_xml", "mapred_site_xml",
        "tez_site_xml", "hms_site_xml", "hive_site_xml",
        "nessie_properties", "flink_conf",
        "prometheus_yml", "loki_yml", "grafana_datasources",
        "dagster_workspace", "dbt_profiles",
        "pipeline_example", "reasoning",
    )
    for field in string_fields:
        m = re.search(rf'"{field}"\s*:\s*"((?:[^"\\]|\\.)*)"', raw, re.DOTALL)
        if m:
            result[field] = m.group(1).replace("\\n", "\n").replace('\\"', '"')

    for obj_field in ("trino_catalogs", "env_vars", "connection_info"):
        m = re.search(rf'"{obj_field}"\s*:\s*(\{{[^}}]+\}})', raw, re.DOTALL)
        if m:
            try:
                result[obj_field] = json.loads(m.group(1))
            except Exception:
                pass

    for arr_field in ("post_start_commands", "connectivity_checks"):
        m = re.search(rf'"{arr_field}"\s*:\s*(\[.*?\])', raw, re.DOTALL)
        if m:
            try:
                result[arr_field] = json.loads(m.group(1))
            except Exception:
                pass

    return result


# ---------------------------------------------------------------------------
# Prompt builder
# ---------------------------------------------------------------------------

def _build_prompt(resolved: list[str], version_overrides: dict[str, str]) -> str:
    topology_lines: list[str] = []
    for cid in resolved:
        comp = COMPONENTS.get(cid, {})
        ver = version_overrides.get(cid, comp.get("default_version", "latest"))
        roles = comp.get("config_roles", [])
        ports = comp.get("ports", [])
        topology_lines.append(
            f"  - {cid} ({comp.get('name', cid)}) v{ver}  roles={roles}  ports={ports}"
        )
    topology = "\n".join(topology_lines)

    def has_role(role: str) -> bool:
        return any(role in COMPONENTS.get(c, {}).get("config_roles", []) for c in resolved)

    has_spark            = has_role("spark_engine")
    has_hudi             = has_role("hudi_format")
    has_delta            = has_role("delta_format")
    has_iceberg          = has_role("iceberg_format")
    has_hdfs             = any(c in resolved for c in ("hdfs", "hadoop-yarn", "hadoop-yarn-nm"))
    has_yarn             = any(c in resolved for c in ("hadoop-yarn", "hadoop-yarn-nm"))
    has_hms              = "hive-metastore" in resolved
    has_nessie           = "nessie" in resolved
    has_iceberg_rest     = "iceberg-rest" in resolved
    has_polaris          = "polaris" in resolved
    has_trino            = any(c in resolved for c in ("trino", "trino-enterprise"))
    has_trino_enterprise = "trino-enterprise" in resolved
    has_kafka            = "kafka" in resolved
    has_flink            = "flink" in resolved
    has_debezium         = "debezium" in resolved
    has_prometheus       = "prometheus" in resolved
    has_grafana          = "grafana" in resolved
    has_loki             = "loki" in resolved
    has_airflow          = "airflow" in resolved
    has_dagster          = "dagster" in resolved
    has_superset         = "superset" in resolved
    has_dbt              = "dbt" in resolved
    has_tez              = "tez" in resolved
    has_hive_server      = "hive" in resolved
    has_ranger           = "ranger-admin" in resolved
    has_openlineage      = "openlineage" in resolved
    has_starrocks        = "starrocks" in resolved
    has_jupyter          = "jupyter" in resolved
    has_minio            = any(c in resolved for c in ("minio", "minio-create-bucket"))

    needed_fields: list[str] = []
    if has_spark:
        needed_fields.append('"spark_defaults": "<full spark-defaults.conf content>"')
    if has_hdfs:
        needed_fields.append('"core_site_xml": "<full core-site.xml XML content>"')
        needed_fields.append('"hdfs_site_xml": "<full hdfs-site.xml XML content>"')
    if has_yarn:
        needed_fields.append('"yarn_site_xml": "<full yarn-site.xml XML content>"')
        needed_fields.append('"mapred_site_xml": "<full mapred-site.xml XML content>"')
    if has_tez:
        needed_fields.append('"tez_site_xml": "<full tez-site.xml XML content>"')
    if has_trino:
        needed_fields.append('"trino_catalogs": {"<catalog_name>": "<.properties content>", ...}')
    if has_hms:
        needed_fields.append('"hms_site_xml": "<full metastore-site.xml XML content>"')
        needed_fields.append('"hive_site_xml": "<full hive-site.xml XML content (connects HiveServer2 and Spark to HMS + HDFS)>"')
    if has_nessie:
        needed_fields.append('"nessie_properties": "<full application.properties content>"')
    if has_flink:
        needed_fields.append('"flink_conf": "<full flink-conf.yaml content>"')
    if has_prometheus:
        needed_fields.append('"prometheus_yml": "<full prometheus.yml with scrape_configs for all running services>"')
    if has_loki:
        needed_fields.append('"loki_yml": "<full loki.yml content>"')
    if has_grafana and has_prometheus:
        needed_fields.append('"grafana_datasources": "<full datasources.yaml content>"')
    if has_dagster:
        needed_fields.append('"dagster_workspace": "<full workspace.yaml content>"')
    if has_dbt:
        needed_fields.append('"dbt_profiles": "<full profiles.yml content>"')
    needed_fields.append('"env_vars": {"KEY": "value", ...}')
    needed_fields.append('"connection_info": {"Service Name": "http://HOST:port", ...}')
    if has_spark:
        needed_fields.append('"pipeline_example": "<PySpark starter snippet>"')
    needed_fields.append(
        '"post_start_commands": [{"description": "...", "container": "udp-<service>", "command": "<shell command inside container>"}, ...]'
    )
    needed_fields.append(
        '"connectivity_checks": [{"name": "...", "command": "docker exec udp-<service> <check command>"}, ...]'
    )

    # Build stack-specific hints
    hints: list[str] = []
    if has_hudi:
        raw = version_overrides.get("spark-hudi") or COMPONENTS.get("spark-hudi", {}).get("default_version", "")
        hudi_ver = raw.split("_")[-1] if "_" in raw else raw
        hints.append(f"- Hudi {hudi_ver}: spark-defaults needs hudi-spark3.5-bundle_{'{'}2.12{'}'}-{hudi_ver}.jar on spark.jars, set spark.serializer=org.apache.spark.serializer.KryoSerializer, hudi.datasource.write.recordkey.field, hudi.datasource.hive_sync.enable=true, hudi.datasource.hive_sync.mode=hms, hudi.datasource.hive_sync.metastore.uris=thrift://udp-hive-metastore:9083")
    if has_delta:
        raw = version_overrides.get("spark-delta") or COMPONENTS.get("spark-delta", {}).get("default_version", "")
        delta_ver = raw.split("_")[-1] if "_" in raw else raw
        hints.append(f"- Delta Lake {delta_ver}: spark-defaults needs spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension, spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog")
    if has_iceberg or has_iceberg_rest:
        hints.append("- Iceberg: spark-defaults needs spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions, configure REST catalog at http://udp-iceberg-rest:8181 or Nessie at http://udp-nessie:19120/api/v1")
    if has_polaris:
        hints.append("- Polaris catalog: REST endpoint http://udp-polaris:8181, requires initial bootstrap credentials from env POLARIS_BOOTSTRAP_CREDENTIALS")
    if has_hdfs:
        hints.append("- HDFS core-site.xml: fs.defaultFS=hdfs://udp-hdfs:8020, hadoop.security.authentication=simple. hdfs-site.xml: dfs.replication=1, dfs.namenode.rpc-address=udp-hdfs:8020, dfs.namenode.http-address=0.0.0.0:9870 (MUST be 0.0.0.0 not udp-hdfs — NameNode binds on all interfaces inside the container), dfs.datanode.http.address=0.0.0.0:9864, dfs.namenode.name.dir=/hadoop/dfs/name, dfs.datanode.data.dir=/hadoop/dfs/data. post_start_commands: hdfs dfs -mkdir -p /user/root /user/spark /user/hive /tmp /warehouse /apps/tez /mr-history/done /mr-history/tmp in udp-hdfs then hdfs dfs -chmod -R 1777 /tmp in udp-hdfs")
    if has_yarn:
        hints.append("- YARN yarn-site.xml: yarn.resourcemanager.hostname=udp-hadoop-yarn, yarn.resourcemanager.webapp.address=0.0.0.0:8088 (MUST be 0.0.0.0 not the hostname — RM binds to container IP otherwise and healthcheck curl localhost:8088 fails), yarn.nodemanager.aux-services=mapreduce_shuffle, yarn.nodemanager.resource.memory-mb=8192, yarn.nodemanager.resource.cpu-vcores=4. mapred-site.xml: mapreduce.framework.name=yarn, mapreduce.jobhistory.address=udp-hadoop-yarn:10020, yarn.app.mapreduce.am.env=HADOOP_MAPRED_HOME=/opt/hadoop. post_start_commands must also create /mr-history dirs on HDFS")
    if has_tez:
        hints.append("- Tez tez-site.xml: tez.lib.uris=hdfs://udp-hdfs:8020/apps/tez/tez-${tez.version}-minimal.tar.gz, tez.use.cluster.hadoop-libs=false, tez.am.resource.memory.mb=2048. post_start_commands: copy Tez tarball from /opt/tez or /usr/local/tez to HDFS /apps/tez/ in udp-hive container, then chmod 755 on the HDFS path")
    if has_hms:
        hints.append("- HMS metastore-site.xml MUST include ALL FOUR JDO properties: (1) javax.jdo.option.ConnectionURL=jdbc:mysql://udp-mysql-hms:3306/metastore?createDatabaseIfNotExist=true&amp;useSSL=false&amp;allowPublicKeyRetrieval=true, (2) javax.jdo.option.ConnectionDriverName=com.mysql.cj.jdbc.Driver, (3) javax.jdo.option.ConnectionUserName=hive, (4) javax.jdo.option.ConnectionPassword=hive_password_pilot. Also: metastore.thrift.uris=thrift://localhost:9083, metastore.warehouse.dir=hdfs://udp-hdfs:8020/warehouse (or s3a://warehouse if MinIO). CRITICAL: in XML use &amp; not & — bare & is invalid XML. hive-site.xml MUST include: hive.metastore.uris=thrift://udp-hive-metastore:9083, hive.server2.thrift.port=10000, hive.server2.authentication=NONE, hive.server2.enable.doAs=false, hive.metastore.schema.verification=false (IMPORTANT: bitsondatadev HMS 3.0 schema != Hive 4.x schema version — disable verification), hive.metastore.warehouse.dir=hdfs://udp-hdfs:8020/warehouse, hive.metastore.event.db.notification.api.auth=false. post_start_commands: /opt/apache-hive-metastore-3.0.0-bin/bin/schematool -dbType mysql -initSchema --verbose in udp-hive-metastore")
    if has_nessie:
        hints.append("- Nessie: NESSIE_VERSION_STORE_TYPE=IN_MEMORY, QUARKUS_OIDC_TENANT_ENABLED=false, REST at http://udp-nessie:19120/api/v1")
    if has_trino and has_hms:
        trino_cid = "trino-enterprise" if has_trino_enterprise else "trino"
        hints.append(f"- Trino ({trino_cid}): hive.properties catalog: connector.name=hive, hive.metastore.uri=thrift://udp-hive-metastore:9083, hive.metastore.thrift.retries=5, fs.native-s3.enabled=true, s3.endpoint=http://udp-minio:9000, s3.aws-access-key=admin, s3.aws-secret-key=udp_admin_12345, s3.path-style-access=true. IMPORTANT: Trino 400+ removed the old hive.s3.* prefix — use s3.* and fs.native-s3.enabled=true instead.")
    if has_trino and has_iceberg_rest:
        hints.append("- Trino iceberg catalog: connector.name=iceberg, iceberg.catalog.type=rest, iceberg.rest-catalog.uri=http://udp-iceberg-rest:8181")
    if has_trino and has_nessie:
        hints.append("- Trino nessie catalog: connector.name=iceberg, iceberg.catalog.type=nessie, iceberg.nessie-catalog.uri=http://udp-nessie:19120/api/v1")
    if has_trino_enterprise and has_starrocks:
        hints.append("- Trino Enterprise starrocks catalog: connector.name=mysql, connection-url=jdbc:mysql://udp-starrocks:9030, connection-user=root, connection-password=")
    if has_ranger:
        hints.append("- Ranger: deployed via kadensungbincho/ranger:2.4.0 community image. Admin at http://udp-ranger-admin:6080 (admin/rangerR0cks!). post_start_commands should wait for Ranger to be healthy (curl /login.jsp) then use Ranger REST API to register HDFS and Hive services. Ranger HDFS plugin XML: ranger-hdfs-security.xml and ranger-hdfs-audit.xml must be generated and mounted in HDFS container (but these are ranger plugin install outputs — note in post_start_commands to configure manually if plugin install fails)")
    if has_kafka:
        hints.append("- Kafka: bootstrap at udp-kafka:29092 (internal), localhost:9092 (external)")
    if has_debezium:
        hints.append("- Debezium: Kafka Connect REST at http://udp-debezium:8083. post_start_commands: register a connector via POST /connectors REST API with PostgreSQL or MySQL source config")
    if has_flink:
        hints.append("- Flink: flink-conf.yaml needs jobmanager.rpc.address: udp-flink, state.backend: filesystem, state.checkpoints.dir: s3://datalake/flink-checkpoints, s3.endpoint: http://udp-minio:9000, s3.access-key: admin, s3.secret-key: udp_admin_12345")
    if has_airflow:
        hints.append("- Airflow 3.x: post_start_commands must run 'airflow db migrate' in udp-airflow. Do NOT run 'airflow users create' — Airflow 3.x uses _AIRFLOW_WWW_USER_CREATE env var for admin creation (the old CLI command was removed in Airflow 3).")
    if has_superset:
        hints.append("- Superset: post_start_commands must run 'superset db upgrade && superset fab create-admin --username admin --firstname Admin --lastname User --email admin@example.com --password admin123 && superset init' in udp-superset")
    if has_openlineage:
        hints.append("- OpenLineage (Marquez): REST at http://udp-openlineage:5000/api/v1. post_start_commands: run Marquez DB migration 'java -jar marquez.jar db migrate conf/marquez.yml' in udp-openlineage. Configure Airflow with AIRFLOW__LINEAGE__BACKEND=openlineage.lineage.backend.OpenLineageBackend and OPENLINEAGE_URL=http://udp-openlineage:5000")
    if has_jupyter:
        hints.append("- JupyterLab: jupyter/all-spark-notebook image, port 8889:8888, token=lakehouse. PySpark pre-installed. AWS_ACCESS_KEY_ID/SECRET point at MinIO. No special config files needed — pipeline_example should show reading/writing Iceberg tables via the REST catalog at http://udp-iceberg-rest:8181.")

    hints_str = "\n".join(hints) if hints else "  (no special hints)"

    return f"""You are an expert datalake administrator. Your job is to configure a complete production lakehouse stack end-to-end — every XML, every JAR reference, every inter-service connection — so ZERO manual steps are needed after docker compose up.

STACK TOPOLOGY:
{topology}

NETWORKING (Docker network: lakehouse — all hostnames = udp-<component-id>):
- MinIO S3:           http://udp-minio:9000   (buckets: s3a://warehouse, s3a://datalake)
- HDFS NameNode:      hdfs://udp-hdfs:8020
- YARN ResourceMgr:   http://udp-hadoop-yarn:8088
- YARN NodeManager:   http://udp-hadoop-yarn-nm:8042
- Hive Metastore:     thrift://udp-hive-metastore:9083
- HiveServer2:        jdbc:hive2://udp-hive:10000
- Nessie REST:        http://udp-nessie:19120/api/v1
- Iceberg REST:       http://udp-iceberg-rest:8181
- Polaris:            http://udp-polaris:8181
- Trino:              http://udp-trino:8080
- Trino Enterprise:   http://udp-trino-enterprise:8080
- Kafka:              udp-kafka:29092 (internal), localhost:9092 (external)
- Debezium Connect:   http://udp-debezium:8083
- Flink JobMgr:       http://udp-flink:8081 (RPC: udp-flink:6123)
- StarRocks FE:       http://udp-starrocks:8030 (MySQL proto: :9030)
- Airflow:            http://udp-airflow:8080
- Ranger Admin:       http://udp-ranger-admin:6080
- Prometheus:         http://udp-prometheus:9090
- Grafana:            http://udp-grafana:3000
- Loki:               http://udp-loki:3100
- OpenLineage:        http://udp-openlineage:5000
- PgBouncer:          udp-pgbouncer:5433

CREDENTIALS (use exactly):
  MinIO:    admin / udp_admin_12345
  MySQL HMS: host=udp-mysql-hms  user=hive  password=hive_password_pilot  db=metastore
  Postgres: host=udp-postgres  user=lakehouse  password=lakehouse_pass  db=lakehouse
  Ranger:   admin / rangerR0cks!
  Grafana:  admin / admin
  Airflow:  admin / admin
  Superset: admin / admin123
  StarRocks: root / (empty)

STACK-SPECIFIC WIRING INSTRUCTIONS:
{hints_str}

Respond with ONLY a valid JSON object with these fields:
{{
{chr(10).join("  " + f for f in needed_fields)}
}}

CRITICAL RULES:
1. Output ONLY valid JSON — no markdown, no explanation outside the JSON.
2. NEVER use triple-double-quotes inside JSON string values. Use single quotes for Python/SQL/XML.
3. All multiline content in JSON strings must use \\n escape sequences.
4. spark_defaults MUST include: fs.s3a.endpoint, fs.s3a.access.key, fs.s3a.secret.key, fs.s3a.path.style.access=true, fs.s3a.impl, AND the correct table format JARs/extensions for the selected formats.
5. core_site_xml and yarn_site_xml MUST use the exact udp-* hostnames from the networking section above.
6. trino_catalogs must have one .properties entry per catalog source (hive, iceberg, starrocks, etc.).
7. hive_site_xml must configure BOTH hive.metastore.uris AND hive.server2 settings.
8. post_start_commands: ORDERED list — earlier commands must complete before later ones. Include: HDFS format, HDFS mkdir, HMS schematool, Tez tarball copy, Airflow migrate, Superset init, Marquez migrate. Use container=udp-<component-id> (e.g. udp-hive-metastore, udp-hdfs, udp-hadoop-yarn).
9. connectivity_checks: cover every critical inter-service dependency — at minimum: MinIO health, HDFS namenode, HMS thrift, Trino /v1/info, YARN cluster info.
10. env_vars must cover ALL credentials, ports, and tunable parameters for every service.
11. connection_info values use HOST as placeholder: e.g. 'http://HOST:8080'.
"""


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def generate_configs(
    resolved: list[str],
    version_overrides: dict[str, str] | None = None,
) -> dict:
    """Call LLM and return a structured config plan for the given component set."""
    version_overrides = version_overrides or {}
    prompt = _build_prompt(resolved, version_overrides)
    return _call_llm(prompt)


def write_configs(
    output_dir: Path,
    config_plan: dict,
    resolved: list[str],
) -> list[str]:
    """Write all config files from *config_plan* into *output_dir*.

    Returns a list of relative paths written.
    """
    written: list[str] = []

    def _write(rel: str, content: str) -> None:
        path = output_dir / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        if rel.endswith(".xml"):
            import re
            # Fix bare & in JDBC URLs that AI sometimes emits without &amp; escaping.
            # Only replace & that are NOT already part of &amp; (or any &name; entity).
            content = re.sub(r'&(?![a-zA-Z#][a-zA-Z0-9#]*;)', '&amp;', content)
        path.write_text(content, encoding="utf-8")
        written.append(rel)

    if sd := config_plan.get("spark_defaults"):
        _write("config/spark/spark-defaults.conf", sd)

    # Hadoop config files (core, HDFS, YARN, MapReduce)
    if cx := config_plan.get("core_site_xml"):
        _write("config/hadoop/core-site.xml", cx)
    if hx := config_plan.get("hdfs_site_xml"):
        _write("config/hadoop/hdfs-site.xml", hx)
    if yx := config_plan.get("yarn_site_xml"):
        _write("config/hadoop/yarn-site.xml", yx)
    if mx := config_plan.get("mapred_site_xml"):
        _write("config/hadoop/mapred-site.xml", mx)

    # Tez
    if tx := config_plan.get("tez_site_xml"):
        _write("config/tez/tez-site.xml", tx)

    # Trino catalogs
    if tc := config_plan.get("trino_catalogs"):
        if isinstance(tc, dict):
            for name, content in tc.items():
                _write(f"config/trino/catalog/{name}.properties", content)

    # Hive Metastore + HiveServer2
    if hms := config_plan.get("hms_site_xml"):
        _write("hive-metastore-site.xml", hms)
    if hv := config_plan.get("hive_site_xml"):
        # Write to hiveserver2-site.xml — the entrypoint overwrites hive-site.xml
        # with its built-in template via envsubst; hiveserver2-site.xml is safe.
        _write("config/hive/hiveserver2-site.xml", hv)

    # Flink
    if fc := config_plan.get("flink_conf"):
        _write("config/flink/flink-conf.yaml", fc)

    # Nessie
    if np := config_plan.get("nessie_properties"):
        _write("nessie.properties", np)

    # Observability
    if py := config_plan.get("prometheus_yml"):
        _write("config/prometheus/prometheus.yml", py)
    if ly := config_plan.get("loki_yml"):
        _write("config/loki/loki.yml", ly)
    if gd := config_plan.get("grafana_datasources"):
        _write("config/grafana/datasources/datasources.yaml", gd)

    # Orchestration / Transform / Lineage
    if dw := config_plan.get("dagster_workspace"):
        _write("dagster/workspace.yaml", dw)
    if dp := config_plan.get("dbt_profiles"):
        _write("config/dbt/profiles.yml", dp)

    # Environment + example
    if ev := config_plan.get("env_vars"):
        if isinstance(ev, dict):
            lines = "\n".join(f"{k}={v}" for k, v in ev.items())
            _write(".env", lines)
    if pe := config_plan.get("pipeline_example"):
        _write("pipeline_example.py", pe)

    # Post-init data consumed by custom_stack_runner._phase_post_cfg()
    if psc := config_plan.get("post_start_commands"):
        if isinstance(psc, list):
            _write("post_init/commands.json", json.dumps(psc, indent=2))
    if cc := config_plan.get("connectivity_checks"):
        if isinstance(cc, list):
            _write("post_init/connectivity_checks.json", json.dumps(cc, indent=2))

    return written
