from __future__ import annotations
import asyncio
import os
import platform
import shlex
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Optional

from . import compose_hardening
from . import credential_gen
from .config import WORK_DIR
from .etl_verify_job import ETL_VERIFY_SPARK_PY
from .events import bus
from .models import LogEvent, StepStatus
from .notifications import notify
from .redact import redact, sanitize_env_overrides, quote_env_value, SECRET_KEYS
from .stack_manifest import StackManifest
from .state import store


_STUDIO_BOOTSTRAP_SH = r"""#!/usr/bin/env bash
# Studio-owned bootstrap. Replaces UDP's scripts/bootstrap.sh because that
# script hard-requires hive-metastore which Studio's v0.3 pilot deliberately
# doesn't ship. This version uses only MinIO + Iceberg-REST + Spark + StarRocks.
set -euo pipefail

# Prevent Git Bash on Windows from converting Unix-style /home/... paths
# into C:/Program Files/Git/home/... before passing them to docker exec.
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL='*'

echo "[studio-bootstrap] waiting for MinIO..."
for i in $(seq 1 60); do
  if curl -fsS http://localhost:9000/minio/health/live >/dev/null 2>&1; then
    echo "  minio OK"; break
  fi
  echo "  ($i/60) minio not ready yet"; sleep 5
  if [ "$i" = "60" ]; then echo "minio never came up"; exit 1; fi
done

echo "[studio-bootstrap] ensuring datalake bucket exists..."
docker start udp-create-bucket 2>/dev/null || true
sleep 8
NETWORK=$(docker inspect udp-minio --format "{{range \$k,\$v := .NetworkSettings.Networks}}{{\$k}}{{end}}" 2>/dev/null | head -1)
docker run --rm --network "${NETWORK:-udp_default}" --entrypoint sh \
  minio/mc:RELEASE.2025-04-16T18-13-26Z \
  -c "mc alias set udp http://minio:9000 admin udp_admin_12345 --api s3v4 && mc mb --ignore-existing udp/datalake" \
  2>/dev/null || echo "  bucket ensure ran (idempotent)"
echo "  datalake bucket ready"

echo "[studio-bootstrap] waiting for Iceberg REST..."
for i in $(seq 1 60); do
  if curl -fsS http://localhost:8181/v1/config >/dev/null 2>&1; then
    echo "  iceberg-rest OK"; break
  fi
  echo "  ($i/60) iceberg-rest not ready yet"; sleep 2
done

echo "[studio-bootstrap] waiting for StarRocks FE..."
for i in $(seq 1 60); do
  if docker exec udp-starrocks-fe mysql -h 127.0.0.1 -P 9030 -u root -e "SELECT 1" >/dev/null 2>&1; then
    echo "  starrocks-fe OK"; break
  fi
  echo "  ($i/60) starrocks-fe not ready yet"; sleep 5
done

echo "[studio-bootstrap] registering StarRocks backend..."
docker exec udp-starrocks-fe mysql -h 127.0.0.1 -P 9030 -u root -e \
  "ALTER SYSTEM ADD BACKEND 'starrocks-be:9050';" 2>&1 | grep -v "already exists" || true

echo "[studio-bootstrap] running Spark bootstrap job (REST-catalog)..."
# Use double-leading-slash on the path so Git Bash on Windows definitively
# doesn't path-convert it. Linux/macOS bash treats // as / so this is safe.
docker exec udp-spark spark-submit //home/iceberg/jobs/bootstrap_demo_lake.py

echo "[studio-bootstrap] creating StarRocks REST catalog (3.3.12+ props)..."
# StarRocks 3.3.12+ fixed PR #55416 — Iceberg REST catalog properties now
# correctly propagate to the S3 FileIO. Required additions vs earlier 3.3.x:
#   - iceberg.catalog.warehouse: explicit warehouse path
#   - iceberg.catalog.vended-credentials-enabled=false: MinIO can't vend
#   - aws.s3.enable_ssl=false: plain HTTP MinIO
docker exec -i udp-starrocks-fe mysql -h 127.0.0.1 -P 9030 -u root <<'SQL'
DROP CATALOG IF EXISTS iceberg_rest_catalog;
CREATE EXTERNAL CATALOG iceberg_rest_catalog
PROPERTIES (
    "type" = "iceberg",
    "iceberg.catalog.type" = "rest",
    "iceberg.catalog.uri" = "http://iceberg-rest:8181",
    "iceberg.catalog.warehouse" = "s3://datalake/warehouse",
    "iceberg.catalog.vended-credentials-enabled" = "false",
    -- StarRocks-native S3 client properties (aws.s3.*) — PR #55416
    -- propagates these to the BE's native S3 reader. Required.
    "aws.s3.endpoint" = "http://minio:9000",
    "aws.s3.enable_ssl" = "false",
    "aws.s3.enable_path_style_access" = "true",
    "aws.s3.region" = "us-east-1",
    "aws.s3.access_key" = "admin",
    "aws.s3.secret_key" = "udp_admin_12345",
    -- Iceberg REST FileIO properties (unprefixed s3.*) — required for
    -- the FileIO layer inside the Iceberg REST client which reads
    -- DIFFERENT property keys than StarRocks's native S3 client.
    -- Without these, FileIO defaults to virtual-hosted-style addressing
    -- which tries `datalake.minio:9000` (no DNS entry) and fails with
    -- UnknownHostException at query time. Same root cause for the
    -- "Windows-only" failure documented in udp-local-v0.2.lock.yaml's
    -- evidence — actually a property propagation bug, not OS-specific.
    -- Fix discovered via StarRocks investigation 2026-05-17 (see
    -- notebook/sessions/2026-05-17-starrocks-minio-investigation.md).
    "s3.endpoint" = "http://minio:9000",
    "s3.path-style-access" = "true",
    "s3.access-key-id" = "admin",
    "s3.secret-access-key" = "udp_admin_12345",
    "client.region" = "us-east-1"
);
SQL

echo "[studio-bootstrap] creating app_analytics views (REST-backed)..."
docker exec -i udp-starrocks-fe mysql -h 127.0.0.1 -P 9030 -u root <<'SQL'
CREATE DATABASE IF NOT EXISTS app_analytics;
DROP VIEW IF EXISTS app_analytics.demo_customer_summary;
CREATE VIEW app_analytics.demo_customer_summary AS
SELECT region, customer_count, total_order_amount, curated_timestamp
FROM iceberg_rest_catalog.curated.demo_customer_summary;
SQL

# Superset is optional — the current UDP compose doesn't ship it, and the
# `docker compose up` service list only starts what's in the cart. Only run
# superset init when the container actually exists in this deployment.
if docker ps -a --format '{{.Names}}' | grep -qx udp-superset; then
  echo "[studio-bootstrap] waiting for Superset..."
  for i in $(seq 1 40); do
    if curl -fsS http://localhost:8088/health >/dev/null 2>&1; then
      echo "  superset container up"; break
    fi
    echo "  ($i/40) superset not ready yet"; sleep 10
    if [ "$i" = "40" ]; then echo "superset never came up"; exit 1; fi
  done

  echo "[studio-bootstrap] initializing Superset DB..."
  docker exec udp-superset superset db upgrade
  echo "[studio-bootstrap] creating Superset admin user..."
  docker exec udp-superset superset fab create-admin \
    --username admin --firstname Admin --lastname User \
    --email admin@example.com --password admin 2>&1 | grep -v "already exist" || true
  echo "[studio-bootstrap] loading Superset roles and permissions..."
  docker exec udp-superset superset init
  echo "  superset init done, waiting for webserver to stabilise..."
  sleep 15
  for i in $(seq 1 12); do
    if curl -fsS http://localhost:8088/health >/dev/null 2>&1; then
      echo "  superset ready: http://localhost:8088 (admin / admin)"; break
    fi
    sleep 5
  done
else
  echo "[studio-bootstrap] superset not part of this deployment — skipping superset init"
fi

echo "[studio-bootstrap] complete"
"""


# ---------------------------------------------------------------------------
# Shared multi-format ETL-verification block.
#
# Writes the format-adaptive PySpark job into udp-spark, runs it for whichever
# table format(s) the user chose in the cart (substituted into __ETLV_FORMATS__
# by the runner), then registers + queries the matching StarRocks external
# catalog. Reused verbatim by BOTH the Spark smoke (_STUDIO_SMOKE_SH) and the
# Trino smoke (_STUDIO_TRINO_SMOKE_SH) so every StarRocks+Spark+MinIO stack
# (Local Demo, Startup Analytics, AI/ML, Fintech, udp-trino) proves all three
# catalogs from a single source of truth.
# ---------------------------------------------------------------------------
_ETL_MULTIFORMAT_BLOCK = r"""# ---- 3-pipeline ETL verification (RDBMS / JSON / MongoDB) --------------------
# Generates >=1000 rows per source in-process, stages the raw files to object
# storage (visible in the console), ingests each into the CHOSEN table format
# via Spark, and fails the smoke test unless all three tables hold >=1000 rows.
#
# Container names / endpoints / catalog names are stack-specific placeholders
# (__SPARK_CTR__, __SR_CTR__, __S3_ENDPOINT__, ...) substituted by the runner
# from _SMOKE_SUBST[stack.id]. Defaults (in _write_studio_bootstrap) reproduce
# the udp-family values exactly, so Local Demo / Startup / AI-ML / Fintech /
# udp-trino render byte-identical to before.
echo "[studio-smoke] writing ETL-verify PySpark job into __SPARK_CTR__..."
docker exec -i __SPARK_CTR__ bash -c 'mkdir -p __SPARK_JOBS__ && cat > __SPARK_JOBS__/lhs_etl_verify.py' <<'PYEOF'
""" + ETL_VERIFY_SPARK_PY + r"""PYEOF

echo "[studio-smoke] running ETL verification for the chosen table format(s): __ETLV_FORMATS__ ..."
# The format(s) the user picked in the cart are substituted into __ETLV_FORMATS__
# by the runner. Delta + Hudi + hadoop-aws are always added at submit time via
# --packages so whichever format was chosen works on this one Spark image:
# Iceberg lands via its REST catalog; Delta/Hudi land as HMS tables on storage.
docker exec __SPARK_EXEC_ENV__ \
  -e ETLV_FORMATS=__ETLV_FORMATS__ -e ETLV_CATALOG=__SPARK_ICE_CAT__ -e ETLV_DB=etl_verify \
  -e ETLV_WAREHOUSE=__WAREHOUSE__ -e ETLV_HMS=__HMS_URI__ -e ETLV_ICE_URI=__ICE_URI__ -e ETLV_ICE_CATALOG_TYPE=__ICE_CAT_TYPE__ \
  -e ETLV_S3_ENDPOINT=__S3_ENDPOINT__ -e ETLV_S3_KEY=__S3_KEY__ -e ETLV_S3_SECRET=__S3_SECRET__ \
  __SPARK_CTR__ __SPARK_SUBMIT__ \
  --packages __SPARK_PACKAGES__ \
  __SPARK_JOBS__/lhs_etl_verify.py

# ---- register + verify the StarRocks catalog for the CHOSEN table format ----
# Iceberg already has __ICEBERG_SR_CAT__ (from bootstrap). Delta/Hudi land in
# the Hive Metastore via the ETL (saveAsTable / hive_sync), so we register the
# matching StarRocks external catalog AFTER the write (registering it before
# would cache stale file listings). Non-fatal: the Spark ETL above is the
# authoritative pass/fail; catalog registration is the "choose a catalog" layer.
ETLV_FMT="__ETLV_FORMATS__"
if echo "$ETLV_FMT" | grep -q iceberg; then
  echo "[studio-smoke] verifying iceberg tables via StarRocks __ICEBERG_SR_CAT__..."
  docker exec -i __SR_CTR__ mysql -h 127.0.0.1 -P 9030 -u root <<'SQL' || echo "  (iceberg cross-check skipped)"
SET new_planner_optimize_timeout=30000;
SELECT 'rdbms' p, COUNT(*) n FROM __ICEBERG_SR_CAT__.etl_verify.rdbms_iceberg
UNION ALL SELECT 'json',  COUNT(*) FROM __ICEBERG_SR_CAT__.etl_verify.json_iceberg
UNION ALL SELECT 'mongo', COUNT(*) FROM __ICEBERG_SR_CAT__.etl_verify.mongo_iceberg;
SQL
fi
if echo "$ETLV_FMT" | grep -q hudi; then
  echo "[studio-smoke] registering + querying StarRocks hudi_catalog..."
  docker exec -i __SR_CTR__ mysql -h 127.0.0.1 -P 9030 -u root <<'SQL' || echo "  (hudi_catalog step skipped)"
SET new_planner_optimize_timeout=30000;
DROP CATALOG IF EXISTS hudi_catalog;
CREATE EXTERNAL CATALOG hudi_catalog PROPERTIES ("type"="hudi","hive.metastore.uris"="__HMS_URI__"__SR_CAT_STORAGE_PROPS__);
SHOW CATALOGS;
SELECT 'rdbms' p, COUNT(*) n FROM hudi_catalog.etl_verify.rdbms_hudi
UNION ALL SELECT 'json',  COUNT(*) FROM hudi_catalog.etl_verify.json_hudi
UNION ALL SELECT 'mongo', COUNT(*) FROM hudi_catalog.etl_verify.mongo_hudi;
SQL
fi
if echo "$ETLV_FMT" | grep -q delta; then
  echo "[studio-smoke] registering StarRocks delta_catalog..."
  docker exec -i __SR_CTR__ mysql -h 127.0.0.1 -P 9030 -u root <<'SQL' || echo "  (delta_catalog step skipped)"
SET new_planner_optimize_timeout=30000;
DROP CATALOG IF EXISTS delta_catalog;
CREATE EXTERNAL CATALOG delta_catalog PROPERTIES ("type"="deltalake","hive.metastore.uris"="__HMS_URI__"__SR_CAT_STORAGE_PROPS__);
SHOW CATALOGS;
SELECT 'rdbms' p, COUNT(*) n FROM delta_catalog.etl_verify.rdbms_delta
UNION ALL SELECT 'json',  COUNT(*) FROM delta_catalog.etl_verify.json_delta
UNION ALL SELECT 'mongo', COUNT(*) FROM delta_catalog.etl_verify.mongo_delta;
SQL
fi
"""


_STUDIO_SMOKE_SH = r"""#!/usr/bin/env bash
# Studio-owned smoke test. Replaces UDP's scripts/smoke-test.sh because that
# script also hard-requires hive-metastore. Validates the same things:
#   - Iceberg raw + curated tables readable from Spark (via REST catalog)
#   - StarRocks can SHOW CATALOGS, SHOW DATABASES, and query the
#     app_analytics view
set -euo pipefail

export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL='*'

echo "[studio-smoke] checking Iceberg REST..."
curl -fsS http://localhost:8181/v1/config >/dev/null || { echo "iceberg-rest unreachable"; exit 1; }
echo "  iceberg-rest OK"

echo "[studio-smoke] checking StarRocks FE..."
docker exec udp-starrocks-fe mysql -h 127.0.0.1 -P 9030 -u root -e "SELECT 1" >/dev/null || { echo "starrocks-fe unreachable"; exit 1; }
echo "  starrocks-fe OK"

echo "[studio-smoke] running Spark Iceberg smoke job..."
docker exec udp-spark spark-submit //home/iceberg/jobs/smoke_test_iceberg.py

echo "[studio-smoke] StarRocks queries..."
# The first query against the Iceberg REST external catalog is a cold read:
# StarRocks fetches table metadata during planning and can blow the default
# 3000ms new_planner_optimize_timeout. Raise it for the session and retry a
# couple of times so a cold catalog doesn't flake the smoke.
SR_SQL="SET new_planner_optimize_timeout=30000; SHOW CATALOGS; SHOW DATABASES; SELECT COUNT(*) AS customer_summary_rows FROM app_analytics.demo_customer_summary;"
for _sr in 1 2 3; do
  if docker exec udp-starrocks-fe mysql -h 127.0.0.1 -P 9030 -u root -e "$SR_SQL"; then
    break
  fi
  [ "$_sr" = "3" ] && { echo "FAIL: StarRocks query after 3 attempts"; exit 1; }
  echo "  StarRocks query attempt $_sr failed (cold catalog) — retrying in 5s..."
  sleep 5
done

if docker ps --format '{{.Names}}' | grep -qx udp-superset; then
  echo "[studio-smoke] checking Superset..."
  curl -fsS http://localhost:8088/health >/dev/null || { echo "superset unreachable on :8088"; exit 1; }
  echo "  superset OK"
else
  echo "[studio-smoke] superset not deployed — skipping check"
fi

""" + _ETL_MULTIFORMAT_BLOCK + r"""
echo "[studio-smoke] passed"
"""


# ---------------------------------------------------------------------------
# Trino candidate stack scripts (udp-trino-local-v0.1)
#
# Mirror shape of the Spark scripts above so the runner harness can reuse its
# result-parsing logic. Key differences from Spark:
#   - Trino's iceberg catalog is configured via a properties file inside the
#     trino container (Trino 475 reads /data/trino/etc/catalog/*.properties
#     only at startup, so the bootstrap writes the file then restarts trino).
#   - Demo seed runs as Trino SQL (CREATE SCHEMA / CREATE TABLE / INSERT)
#     instead of a PySpark job; round-trip raw -> curated stays inside Trino.
#   - StarRocks side of the bootstrap is identical to v0.2 (same Iceberg-REST
#     endpoint, same 3.3.12+ catalog properties): both engines read the same
#     warehouse, so anything Trino writes is visible from StarRocks.
# Promotion to pilot-stable still requires a real end-to-end install with
# evidence captured into stacks/compatibility/udp-trino-local-v0.1.lock.yaml.
# ---------------------------------------------------------------------------


_STUDIO_TRINO_BOOTSTRAP_SH = r"""#!/usr/bin/env bash
# Studio-owned bootstrap for the Trino candidate stack. Configures Trino's
# Iceberg-REST catalog, seeds demo raw/curated tables via Trino SQL, and
# wires StarRocks's external catalog at the SAME Iceberg-REST endpoint so
# both engines see the same warehouse.
set -euo pipefail

export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL='*'

echo "[studio-trino-bootstrap] waiting for MinIO..."
for i in $(seq 1 60); do
  if curl -fsS http://localhost:9000/minio/health/live >/dev/null 2>&1; then
    echo "  minio OK"; break
  fi
  echo "  ($i/60) minio not ready yet"; sleep 5
  if [ "$i" = "60" ]; then echo "minio never came up"; exit 1; fi
done

echo "[studio-trino-bootstrap] ensuring datalake bucket exists..."
docker start udp-create-bucket 2>/dev/null || true
sleep 8
NETWORK=$(docker inspect udp-minio --format "{{range \$k,\$v := .NetworkSettings.Networks}}{{\$k}}{{end}}" 2>/dev/null | head -1)
docker run --rm --network "${NETWORK:-udp_default}" --entrypoint sh \
  minio/mc:RELEASE.2025-04-16T18-13-26Z \
  -c "mc alias set udp http://minio:9000 admin udp_admin_12345 --api s3v4 && mc mb --ignore-existing udp/datalake" \
  2>/dev/null || echo "  bucket ensure ran (idempotent)"
echo "  datalake bucket ready"

echo "[studio-trino-bootstrap] waiting for Iceberg REST..."
for i in $(seq 1 60); do
  if curl -fsS http://localhost:8181/v1/config >/dev/null 2>&1; then
    echo "  iceberg-rest OK"; break
  fi
  echo "  ($i/60) iceberg-rest not ready yet"; sleep 2
done

echo "[studio-trino-bootstrap] waiting for Trino..."
for i in $(seq 1 60); do
  if docker exec udp-trino curl -fsS http://localhost:8080/v1/info >/dev/null 2>&1; then
    echo "  trino OK"; break
  fi
  echo "  ($i/60) trino not ready yet"; sleep 5
done

echo "[studio-trino-bootstrap] writing Trino iceberg catalog properties..."
# Trino 475 reads /data/trino/etc/catalog/*.properties at startup. We write
# the file then restart trino so the iceberg catalog is registered. Idempotent
# — writing the same file twice is fine; restart is cheap on a warm host.
# Path-style + explicit S3 credentials required by MinIO (HTTP, no IAM).
docker exec udp-trino mkdir -p /data/trino/etc/catalog/
docker exec -i udp-trino bash -c 'cat > /data/trino/etc/catalog/iceberg.properties' <<'TRINOCAT'
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://iceberg-rest:8181
iceberg.rest-catalog.warehouse=s3://datalake/warehouse
fs.native-s3.enabled=true
s3.endpoint=http://minio:9000
s3.region=us-east-1
s3.path-style-access=true
s3.aws-access-key=admin
s3.aws-secret-key=udp_admin_12345
TRINOCAT

# Defense: confirm the heredoc actually reached the container. A missing `-i`
# on `docker exec` silently produces an empty file, which then crashes Trino
# at startup with "Catalog configuration ... does not contain connector.name".
# Fail fast here rather than waiting ~10 min for Trino to enter a restart loop.
docker exec udp-trino test -s /data/trino/etc/catalog/iceberg.properties \
  || { echo "iceberg.properties wrote empty — bootstrap aborted"; exit 1; }

# --- OpenLineage: wire Trino to EMIT lineage into Marquez ---------------------
# Only runs when the openlineage (Marquez) container is part of this stack
# (e.g. Fintech Compliance) — no-op for plain Trino stacks. Trino 466+ ships a
# built-in `openlineage` event listener; pointing it at Marquez's
# /api/v1/lineage endpoint means every query the bootstrap + smoke run below
# (CREATE TABLE AS SELECT, INSERT, the smoke SELECTs) is captured as a lineage
# job + dataset graph you can browse in the Marquez UI (namespace: trino-demo).
if docker ps --format '{{.Names}}' | grep -qx udp-openlineage; then
  echo "[studio-trino-bootstrap] wiring Trino -> OpenLineage (Marquez)..."
  docker exec -i udp-trino bash -c 'cat > /data/trino/etc/openlineage-event-listener.properties' <<'OLCFG'
event-listener.name=openlineage
openlineage-event-listener.transport.type=HTTP
openlineage-event-listener.transport.url=http://udp-openlineage:5000
openlineage-event-listener.transport.endpoint=/api/v1/lineage
openlineage-event-listener.trino.uri=http://udp-trino:8080
openlineage-event-listener.namespace=trino-demo
OLCFG
  docker exec udp-trino bash -c 'grep -q openlineage-event-listener /data/trino/etc/config.properties 2>/dev/null || echo "event-listener.config-files=/data/trino/etc/openlineage-event-listener.properties" >> /data/trino/etc/config.properties'
  echo "  Trino OpenLineage event listener configured (namespace: trino-demo)"
fi

echo "[studio-trino-bootstrap] restarting Trino to load iceberg catalog..."
# NOTE: `docker compose restart trino` would fail here because the bootstrap
# script runs without the `-f docker-compose.fragment.yml` flag, so compose
# only sees the base manifest (no trino service) and rejects the command.
# Use `docker restart <container_name>` directly — bypasses compose entirely.
docker restart udp-trino

echo "[studio-trino-bootstrap] waiting for Trino after restart..."
TRINO_BACK=no
for i in $(seq 1 24); do
  if docker exec udp-trino curl -fsS http://localhost:8080/v1/info >/dev/null 2>&1; then
    echo "  trino back up"; TRINO_BACK=yes; break
  fi
  echo "  ($i/24) trino not ready yet"; sleep 5
done
# Safety net: if Trino did NOT come back and we added the OpenLineage listener,
# a bad listener config is the most likely cause — strip it and restart so the
# install never bricks on lineage wiring (worst case: no lineage, working Trino).
if [ "$TRINO_BACK" = "no" ] && docker exec udp-trino test -f /data/trino/etc/openlineage-event-listener.properties 2>/dev/null; then
  echo "  Trino didn't return — removing OpenLineage listener and restarting (lineage disabled, stack still works)"
  docker exec udp-trino rm -f /data/trino/etc/openlineage-event-listener.properties
  docker exec udp-trino bash -c "sed -i '/openlineage-event-listener/d' /data/trino/etc/config.properties"
  docker restart udp-trino
  for i in $(seq 1 24); do
    docker exec udp-trino curl -fsS http://localhost:8080/v1/info >/dev/null 2>&1 && { echo "  trino back up (without lineage)"; break; }
    echo "  ($i/24) trino not ready yet"; sleep 5
  done
fi

echo "[studio-trino-bootstrap] verifying iceberg catalog is registered..."
for i in $(seq 1 12); do
  if docker exec -e JAVA_TOOL_OPTIONS= udp-trino trino --execute "SHOW CATALOGS" 2>/dev/null | grep -q "^iceberg$"; then
    echo "  iceberg catalog visible"; break
  fi
  echo "  ($i/12) iceberg catalog not yet visible"; sleep 5
done

echo "[studio-trino-bootstrap] seeding demo schemas + tables via Trino..."
docker exec -e JAVA_TOOL_OPTIONS= -i udp-trino trino <<'SQL'
CREATE SCHEMA IF NOT EXISTS iceberg.raw;
CREATE SCHEMA IF NOT EXISTS iceberg.curated;

DROP TABLE IF EXISTS iceberg.raw.demo_customers;
CREATE TABLE iceberg.raw.demo_customers (
  customer_id BIGINT,
  region VARCHAR,
  order_amount DECIMAL(10,2),
  ingested_at TIMESTAMP(6)
);

INSERT INTO iceberg.raw.demo_customers VALUES
  (BIGINT '1', 'us-east',    DECIMAL '120.50', current_timestamp),
  (BIGINT '2', 'us-west',    DECIMAL '300.00', current_timestamp),
  (BIGINT '3', 'eu-central', DECIMAL '75.25',  current_timestamp),
  (BIGINT '4', 'us-east',    DECIMAL '420.99', current_timestamp),
  (BIGINT '5', 'apac',       DECIMAL '199.99', current_timestamp);

DROP TABLE IF EXISTS iceberg.curated.demo_customer_summary;
CREATE TABLE iceberg.curated.demo_customer_summary AS
SELECT
  region,
  CAST(COUNT(*) AS BIGINT)             AS customer_count,
  SUM(order_amount)                    AS total_order_amount,
  current_timestamp                    AS curated_timestamp
FROM iceberg.raw.demo_customers
GROUP BY region;
SQL

echo "[studio-trino-bootstrap] waiting for StarRocks FE..."
for i in $(seq 1 60); do
  if docker exec udp-starrocks-fe mysql -h 127.0.0.1 -P 9030 -u root -e "SELECT 1" >/dev/null 2>&1; then
    echo "  starrocks-fe OK"; break
  fi
  echo "  ($i/60) starrocks-fe not ready yet"; sleep 5
done

echo "[studio-trino-bootstrap] registering StarRocks backend..."
docker exec udp-starrocks-fe mysql -h 127.0.0.1 -P 9030 -u root -e \
  "ALTER SYSTEM ADD BACKEND 'starrocks-be:9050';" 2>&1 | grep -v "already exists" || true

echo "[studio-trino-bootstrap] creating StarRocks REST catalog (shared with Trino)..."
# Same Iceberg-REST endpoint as Trino above — both engines see the same
# warehouse. PR #55416 (3.3.12+) makes catalog properties propagate to FileIO.
docker exec -i udp-starrocks-fe mysql -h 127.0.0.1 -P 9030 -u root <<'SQL'
DROP CATALOG IF EXISTS iceberg_rest_catalog;
CREATE EXTERNAL CATALOG iceberg_rest_catalog
PROPERTIES (
    "type" = "iceberg",
    "iceberg.catalog.type" = "rest",
    "iceberg.catalog.uri" = "http://iceberg-rest:8181",
    "iceberg.catalog.warehouse" = "s3://datalake/warehouse",
    "iceberg.catalog.vended-credentials-enabled" = "false",
    -- Same dual-property pattern as udp-local-v0.2 bootstrap above.
    -- aws.s3.* for StarRocks-native S3 client (PR #55416);
    -- s3.* unprefixed for the Iceberg REST FileIO layer.
    "aws.s3.endpoint" = "http://minio:9000",
    "aws.s3.enable_ssl" = "false",
    "aws.s3.enable_path_style_access" = "true",
    "aws.s3.region" = "us-east-1",
    "aws.s3.access_key" = "admin",
    "aws.s3.secret_key" = "udp_admin_12345",
    "s3.endpoint" = "http://minio:9000",
    "s3.path-style-access" = "true",
    "s3.access-key-id" = "admin",
    "s3.secret-access-key" = "udp_admin_12345",
    "client.region" = "us-east-1"
);
SQL

echo "[studio-trino-bootstrap] creating app_analytics views (REST-backed)..."
docker exec -i udp-starrocks-fe mysql -h 127.0.0.1 -P 9030 -u root <<'SQL'
CREATE DATABASE IF NOT EXISTS app_analytics;
DROP VIEW IF EXISTS app_analytics.demo_customer_summary;
CREATE VIEW app_analytics.demo_customer_summary AS
SELECT region, customer_count, total_order_amount, curated_timestamp
FROM iceberg_rest_catalog.curated.demo_customer_summary;
SQL

echo "[studio-trino-bootstrap] complete"
"""


_STUDIO_TRINO_SMOKE_SH = r"""#!/usr/bin/env bash
# Studio-owned smoke test for the Trino candidate stack. Validates:
#   - Iceberg REST + Trino + StarRocks FE all reachable
#   - Trino can read the curated table the bootstrap seeded
#   - StarRocks can read the SAME table via its REST-backed external catalog
#     (proves the cross-engine view is consistent against one warehouse)
set -euo pipefail

export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL='*'

echo "[studio-trino-smoke] checking Iceberg REST..."
curl -fsS http://localhost:8181/v1/config >/dev/null || { echo "iceberg-rest unreachable"; exit 1; }
echo "  iceberg-rest OK"

echo "[studio-trino-smoke] checking Trino..."
docker exec udp-trino curl -fsS http://localhost:8080/v1/info >/dev/null || { echo "trino unreachable"; exit 1; }
echo "  trino OK"

echo "[studio-trino-smoke] checking StarRocks FE..."
docker exec udp-starrocks-fe mysql -h 127.0.0.1 -P 9030 -u root -e "SELECT 1" >/dev/null || { echo "starrocks-fe unreachable"; exit 1; }
echo "  starrocks-fe OK"

echo "[studio-trino-smoke] Trino round-trip query (curated table)..."
docker exec -e JAVA_TOOL_OPTIONS= udp-trino trino --execute \
  "SELECT region, customer_count, total_order_amount FROM iceberg.curated.demo_customer_summary ORDER BY region"

echo "[studio-trino-smoke] StarRocks queries (same Iceberg catalog)..."
docker exec udp-starrocks-fe mysql -h 127.0.0.1 -P 9030 -u root -e \
  "SHOW CATALOGS; SHOW DATABASES; SELECT COUNT(*) AS customer_summary_rows FROM app_analytics.demo_customer_summary;"

""" + _ETL_MULTIFORMAT_BLOCK + r"""
echo "[studio-trino-smoke] passed"
"""


# Map stack id → (bootstrap script body, smoke script body). The runner writes
# the pair matching the install's stack id into install_dir/scripts/ as the
# names the manifest's `commands.bootstrap`/`commands.smoke` argv reference.
_STUDIO_SCRIPT_SETS: dict[str, tuple[tuple[str, str], tuple[str, str]]] = {
    "udp-local-v0.2": (
        ("lhs-bootstrap.sh", _STUDIO_BOOTSTRAP_SH),
        ("lhs-smoke.sh",     _STUDIO_SMOKE_SH),
    ),
    # Startup Analytics = udp-local-v0.2 core + Superset (from the compose
    # fragment). Same REST-catalog bootstrap; its Superset init block runs
    # because the udp-superset container exists for this stack.
    "startup-analytics-local-v0.1": (
        ("lhs-bootstrap.sh", _STUDIO_BOOTSTRAP_SH),
        ("lhs-smoke.sh",     _STUDIO_SMOKE_SH),
    ),
    "udp-trino-local-v0.1": (
        ("lhs-trino-bootstrap.sh", _STUDIO_TRINO_BOOTSTRAP_SH),
        ("lhs-trino-smoke.sh",     _STUDIO_TRINO_SMOKE_SH),
    ),
    # AI/ML Research = udp-trino runtime + Spark (declared) + JupyterLab.
    # Reuses the Trino bootstrap/smoke; Spark + Jupyter read the same
    # Iceberg-REST warehouse the Trino bootstrap seeds.
    "ai-ml-research-local-v0.1": (
        ("lhs-trino-bootstrap.sh", _STUDIO_TRINO_BOOTSTRAP_SH),
        ("lhs-trino-smoke.sh",     _STUDIO_TRINO_SMOKE_SH),
    ),
    "fintech-compliance-local-v0.1": (
        ("lhs-trino-bootstrap.sh", _STUDIO_TRINO_BOOTSTRAP_SH),
        ("lhs-trino-smoke.sh",     _STUDIO_TRINO_SMOKE_SH),
    ),
}

# v0.6 candidate stacks ship their bootstrap/smoke bodies from a separate
# module to keep this file lean. The merge below is the single integration
# point — anything keyed by a v0.6 stack id is resolved by _write_studio_bootstrap
# via the same dispatch path as the existing two.
try:
    from .runner_extra_scripts import EXTRA_SCRIPT_SETS as _EXTRA_SCRIPT_SETS
    _STUDIO_SCRIPT_SETS.update(_EXTRA_SCRIPT_SETS)
except ImportError:
    # Module is optional; if absent, the v0.6 candidate stacks fall back
    # to whatever the manifest's commands.bootstrap/smoke argv points at.
    pass


# Per-stack substitution values for the shared _ETL_MULTIFORMAT_BLOCK. The
# DEFAULT reproduces the udp-family values exactly (container names, MinIO
# endpoint/creds, Spark iceberg catalog `udp`, StarRocks iceberg catalog
# `iceberg_rest_catalog`) so Local Demo / Startup / AI-ML / Fintech / udp-trino
# render byte-identical to the pre-parameterization block. Stacks with a
# different shape (different container prefix, MinIO creds, warehouse bucket)
# override only the keys that differ.
_SMOKE_SUBST_DEFAULT: dict[str, str] = {
    # NOTE: __SR_CAT_STORAGE_PROPS__ must be FIRST — its value embeds __S3_*__
    # placeholders that the later entries resolve (substitution is order-sensitive).
    # S3/MinIO stacks append the aws.s3.* block; HDFS stacks override it to "".
    "__SR_CAT_STORAGE_PROPS__": (
        ',"aws.s3.endpoint"="__S3_ENDPOINT__","aws.s3.enable_ssl"="false",'
        '"aws.s3.enable_path_style_access"="true","aws.s3.access_key"="__S3_KEY__",'
        '"aws.s3.secret_key"="__S3_SECRET__","aws.s3.region"="us-east-1"'
    ),
    "__SPARK_CTR__":      "udp-spark",
    "__SR_CTR__":         "udp-starrocks-fe",
    "__SPARK_JOBS__":     "//home/iceberg/jobs",
    "__SPARK_ICE_CAT__":  "udp",
    "__WAREHOUSE__":      "s3a://datalake/warehouse",
    "__S3_ENDPOINT__":    "http://minio:9000",
    "__S3_KEY__":         "admin",
    "__S3_SECRET__":      "udp_admin_12345",
    "__HMS_URI__":        "thrift://hive-metastore:9083",
    "__ICEBERG_SR_CAT__": "iceberg_rest_catalog",
    # spark-submit is on PATH in the tabulario image (udp-family). HDFS stacks on
    # apache/spark override with the absolute path (/opt/spark/bin/spark-submit).
    "__SPARK_SUBMIT__":   "spark-submit",
    # Extra `docker exec` flags before the container name. Empty for udp
    # (tabulario has a writable HOME/.ivy2). apache/spark needs HOME=/tmp so
    # Ivy (--packages resolution) has a writable cache.
    "__SPARK_EXEC_ENV__": "",
    # Empty = rely on the container's pre-baked iceberg catalog (udp-family).
    "__ICE_URI__":        "",
    # Iceberg catalog type for the ETL job: "rest" (default) or "hive" (HDFS
    # stacks reuse their Hive Metastore as the iceberg catalog).
    "__ICE_CAT_TYPE__":   "rest",
    # Spark --packages for the ETL. Default = Spark 3.5 (tabulario image). HDFS
    # stacks on apache/spark 3.4 override with the 3.4-compatible bundle set.
    "__SPARK_PACKAGES__": (
        "io.delta:delta-spark_2.12:3.2.1,"
        "org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0,"
        "org.apache.hadoop:hadoop-aws:3.3.4"
    ),
}
_SMOKE_SUBST: dict[str, dict[str, str]] = {
    # Streaming Lakehouse — same architecture as udp (tabulario Spark image +
    # StarRocks FE + MinIO + Iceberg REST) but every service uses the `sl-`
    # container prefix, MinIO secret `streaming123`, Spark iceberg catalog `rest`
    # and warehouse bucket `streaming-lake`. HMS is added additively to the
    # stack's own compose; the Kafka/Flink build is untouched.
    "streaming-local-v1.0": {
        "__SPARK_CTR__":     "sl-spark",
        "__SR_CTR__":        "sl-starrocks-fe",
        "__SPARK_ICE_CAT__": "rest",
        "__WAREHOUSE__":     "s3a://streaming-lake/warehouse",
        "__S3_ENDPOINT__":   "http://sl-minio:9000",
        "__S3_SECRET__":     "streaming123",
        "__ICEBERG_SR_CAT__": "iceberg_rest_catalog",
        # sl-spark has no udp-patched spark-defaults; point iceberg at the
        # stack's own REST catalog so the iceberg pipeline works too.
        "__ICE_URI__":       "http://sl-iceberg-rest:8181",
    },
    # Production Lakehouse — udp-family container names + MinIO(datalake) so most
    # keys are the DEFAULT. Only differs in: no baked Spark catalog (added Spark
    # points at Nessie's iceberg REST endpoint), and StarRocks' iceberg catalog
    # is `iceberg_nessie_catalog` (registered by the Nessie bootstrap).
    "iceberg-nessie-trino-local-v0.1": {
        "__SPARK_ICE_CAT__":  "nessie",
        "__ICE_URI__":        "http://nessie:19120/iceberg/main",
        "__ICEBERG_SR_CAT__": "iceberg_nessie_catalog",
    },
    # Enterprise / Healthcare (enterprise-hadoop-v1.0) — HDFS-NATIVE, no MinIO.
    # Reuses the stack's OWN Postgres-backed Hive Metastore + apache/spark 3.4;
    # data lands on HDFS. iceberg via a Hive catalog (no REST); Delta/Hudi in the
    # existing HMS. StarRocks catalog props carry NO aws.s3.* (HDFS via HMS).
    # NOTE: not yet live-verified — this box can't run the 20GB HDFS build;
    # proven on a real 20GB+ target via SSH install.
    "enterprise-hadoop-v1.0": {
        "__SPARK_CTR__":          "ehd-spark",
        "__SR_CTR__":             "ehd-starrocks-fe",
        "__SPARK_SUBMIT__":       "/opt/spark/bin/spark-submit",
        "__SPARK_EXEC_ENV__":     "-e HOME=/tmp",
        "__SPARK_JOBS__":         "/tmp",
        "__SPARK_ICE_CAT__":      "ice",
        "__WAREHOUSE__":          "hdfs://namenode:9820/tmp/hive/warehouse",
        "__S3_ENDPOINT__":        "",
        "__S3_KEY__":             "",
        "__S3_SECRET__":          "",
        "__ICE_URI__":            "",
        "__ICE_CAT_TYPE__":       "hive",
        "__ICEBERG_SR_CAT__":     "iceberg_hive_catalog",
        "__SR_CAT_STORAGE_PROPS__": "",
        "__SPARK_PACKAGES__": (
            "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.5.2,"
            "io.delta:delta-spark_2.12:2.4.0,"
            "org.apache.hudi:hudi-spark3.4-bundle_2.12:0.15.0"
        ),
    },
}

# Stacks that own their bootstrap/smoke scripts (not generated from
# _STUDIO_SCRIPT_SETS) but should still get the additive 3-catalog feature: the
# runner drops a standalone `lhs-etl-verify.sh` (the shared ETL block, values
# substituted) into scripts/, and the stack's own smoke calls it at the end.
# Keeps the ETL PySpark job DRY (single source: etl_verify_job.py).
_ETL_FEATURE_STACKS: set[str] = {
    "streaming-local-v1.0",
    "iceberg-nessie-trino-local-v0.1",
    "enterprise-hadoop-v1.0",
}


def _build_steps(stack: StackManifest) -> list[StepStatus]:
    if stack.is_remote_cluster:
        return [
            StepStatus(id="verify", title="Verify cluster connectivity"),
            StepStatus(id="finalize", title="Capture outputs"),
        ]
    return [
        StepStatus(id="prepare", title="Prepare workspace"),
        StepStatus(id="clone", title="Clone UDP repository"),
        StepStatus(id="env", title="Write .env file"),
        StepStatus(id="doctor", title="Run doctor checks"),
        StepStatus(id="start", title="Start stack (docker compose up)"),
        StepStatus(id="bootstrap", title="Bootstrap demo lakehouse"),
        StepStatus(id="smoke", title="Run smoke tests"),
        StepStatus(id="finalize", title="Capture outputs"),
    ]


def _iter_bash_candidates() -> list[str]:
    """Every `bash` on PATH, in PATH order, plus well-known Git-for-Windows
    install locations that may not be on PATH.

    On Windows, `C:\\Windows\\System32\\bash.exe` is the WSL launcher shim.
    It frequently resolves ahead of Git Bash in a process's PATH (e.g. when
    spawned from PowerShell), and fails outright on any machine where WSL
    is present but has no Linux distro installed — even though a perfectly
    good Git Bash is installed and on PATH. shutil.which() alone can't tell
    the difference, so every candidate must actually be invoked and
    verified (see _bash_executable below) rather than trusting the first
    PATH match."""
    seen: set[str] = set()
    candidates: list[str] = []
    names = ("bash.exe", "bash") if platform.system() == "Windows" else ("bash",)
    for d in os.environ.get("PATH", "").split(os.pathsep):
        if not d:
            continue
        for name in names:
            p = Path(d) / name
            if p.is_file():
                key = str(p).lower()
                if key not in seen:
                    seen.add(key)
                    candidates.append(str(p))
    if platform.system() == "Windows":
        for extra in (
            r"C:\Program Files\Git\bin\bash.exe",
            r"C:\Program Files\Git\usr\bin\bash.exe",
            r"C:\Program Files (x86)\Git\bin\bash.exe",
        ):
            key = extra.lower()
            if key not in seen and Path(extra).is_file():
                seen.add(key)
                candidates.append(extra)
    return candidates


def _bash_executable() -> str:
    candidates = _iter_bash_candidates()
    if not candidates:
        raise RuntimeError(
            "bash not found in PATH. Install Git Bash (Windows) or any POSIX bash."
        )
    for c in candidates:
        try:
            r = subprocess.run([c, "--version"], capture_output=True, timeout=5)
            if r.returncode == 0:
                return c
        except Exception:
            continue
    raise RuntimeError(
        "bash was found on PATH but every candidate failed to run "
        f"({', '.join(candidates)}). On Windows this is usually the WSL "
        "launcher shim at System32\\bash.exe shadowing Git Bash — install "
        "Git for Windows or remove/reorder the broken WSL entry."
    )


def _to_posix_path(p: Path) -> str:
    """On Windows, bash needs /c/Users/... not C:\\Users\\...

    Guards: only handle absolute drive-letter paths (C:\\…). Refuses UNC
    (\\\\server\\share) and long-path-prefixed (\\\\?\\) paths; falls back to
    the raw string for non-Windows.
    """
    if platform.system() != "Windows":
        return str(p)
    s = str(Path(p).resolve())
    # UNC / long-path / weird: bail out by returning the original string.
    # Bash inside Git for Windows can usually handle forward-slashed paths.
    if s.startswith("\\\\") or len(s) < 3 or s[1] != ":":
        return s.replace("\\", "/")
    drive = s[0].lower()
    rest = s[2:].replace("\\", "/")
    if not rest.startswith("/"):
        rest = "/" + rest
    return f"/{drive}{rest}"


# Env vars to pass to child subprocesses. Keep the surface small; explicitly
# drop credentials present in the parent process env (CI tokens, AWS keys, etc.).
_ENV_ALLOW = {
    "PATH", "HOME", "USER", "USERNAME", "USERPROFILE", "LANG", "LC_ALL", "TZ",
    "TMP", "TEMP", "TMPDIR",
    # Docker on Windows / WSL
    "DOCKER_HOST", "DOCKER_CONTEXT", "DOCKER_TLS_VERIFY", "DOCKER_CERT_PATH",
    # MSYS / Git Bash
    "MSYSTEM", "MSYS", "MSYSTEM_PREFIX", "MINGW_PREFIX",
    # Locale needed by docker compose
    "COLUMNS", "LINES", "TERM",
    # systemroot is needed for various Windows shell utilities
    "SYSTEMROOT", "SYSTEMDRIVE", "COMSPEC", "WINDIR", "PROGRAMFILES", "PROGRAMFILES(X86)",
    # Compose-fragment host-port + tuning overrides (non-secret) that
    # stack_compose_fragments.py interpolates as `${VAR:-default}`. Passing
    # these through lets an operator dodge host-port conflicts (e.g. Marquez's
    # 5000 colliding with another app) without editing any file. Credentials
    # are still dropped — only ports/opts are whitelisted here.
    "MARQUEZ_HTTP_PORT", "MARQUEZ_ADMIN_PORT", "MARQUEZ_WEB_PORT",
    "TRINO_HTTP_PORT", "TRINO_JAVA_OPTS",
    "TRINO_QUERY_MAX_MEMORY", "TRINO_QUERY_MAX_MEMORY_PER_NODE",
}


def _is_truthy(value: Any) -> bool:
    """Permissive env-flag parser. None/empty/"0"/"false"/"no"/"off" → False;
    everything else (including bare presence) → True."""
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    s = str(value).strip().lower()
    if not s:
        return False
    return s not in {"0", "false", "no", "off", "disable", "disabled"}


def _build_subprocess_env() -> dict[str, str]:
    src = os.environ
    out = {k: v for k, v in src.items() if k in _ENV_ALLOW or k.startswith("LHS_")}
    out["PYTHONUNBUFFERED"] = "1"
    out["GIT_TERMINAL_PROMPT"] = "0"
    # docker compose v2 needs HOME
    out.setdefault("HOME", src.get("HOME", src.get("USERPROFILE", "")))
    return out


def _force_remove_tree(path: Path) -> None:
    r"""Remove a directory tree robustly, defeating Windows edge cases.

    shutil.rmtree — and even `rd /s /q` — choke on artifacts that Airflow /
    Docker routinely leave under an install dir:
      * reparse points / symlinks (e.g. scheduler/logs/latest)
      * reserved device-name files (a literal `nul`, `con`, `aux` — created by
        a shell redirect that hit a non-device context). These cannot even be
        opened or deleted through normal Win32 path parsing.
    robocopy mirroring an EMPTY directory into the target reliably empties even
    those, because robocopy uses the \\?\ extended-length path API internally.
    We then remove the emptied husk. Raises with a clear message if the tree
    still can't be removed (so the clone step fails loudly, not cryptically).
    """
    try:
        shutil.rmtree(path)
        return
    except OSError:
        if sys.platform != "win32":
            # Linux/macOS: a container running as root can write into a
            # bind-mounted install dir (e.g. enterprise-hadoop's prefetch drops
            # Hudi/Spark jars into ./jars as root), leaving files the Studio
            # user can't delete -> PermissionError on the NEXT install's clone.
            # Delete the tree via a throwaway root container that bind-mounts
            # the PARENT: docker runs as root, so it can remove anything. No
            # passwordless sudo required.
            try:
                parent = path.parent
                subprocess.run(
                    ["docker", "run", "--rm", "-v", f"{parent}:/w",
                     "alpine", "sh", "-c", f"rm -rf /w/{shlex.quote(path.name)}"],
                    capture_output=True, timeout=180,
                )
            except Exception:
                pass
            if path.exists():
                raise RuntimeError(
                    f"could not remove existing install dir {path}: it contains "
                    f"root-owned files (a container wrote into a bind mount as "
                    f"root) and the docker-based cleanup failed. Remove it "
                    f"manually with `sudo rm -rf {path}`, then retry."
                )
            return
    # Windows fallback: robocopy /MIR from an empty dir, then rmdir the husk.
    import tempfile as _tf
    empty = Path(_tf.mkdtemp(prefix="lhs-empty-"))
    try:
        # robocopy exit codes 0-7 are success/informational — do NOT check=True.
        subprocess.run(
            ["robocopy", str(empty), str(path), "/MIR",
             "/NFL", "/NDL", "/NJH", "/NJS", "/NC", "/NS", "/NP"],
            capture_output=True,
        )
        subprocess.run(["cmd", "/c", "rd", "/s", "/q", str(path)],
                       capture_output=True)
    finally:
        try:
            os.rmdir(empty)
        except OSError:
            pass
    if path.exists():
        raise RuntimeError(
            f"could not remove existing install dir {path}: a locked file or "
            f"Windows reserved-name artifact (e.g. 'nul') remains. Stop any "
            f"container/process using it, or delete the folder manually, then "
            f"retry."
        )


class UDPRunner:
    def __init__(self, stack: StackManifest, install_id: str, host: str, install_dir: Path):
        self.stack = stack
        self.install_id = install_id
        self.host = host
        self.install_dir = install_dir
        self._proc: Optional[asyncio.subprocess.Process] = None
        self._cancel = False
        # v0.6.1 optional compose overlays (Airflow / Dagster / Superset).
        # Each entry: {"file": Path, "services": [str, ...], "name": str}.
        # Populated by _write_optional_overlays during the env step when
        # the matching LHS_*_ENABLED env flag is set; consumed by the
        # docker_compose_up branch of _step_cmd to inject `-f overlay.yml`
        # and append the overlay's services to the `up -d` argv.
        self._overlays: list[dict[str, Any]] = []

    # ---------- event helpers ----------

    def _emit(self, kind: str, **kwargs) -> None:
        evt = LogEvent(install_id=self.install_id, ts=time.time(), kind=kind, **kwargs)  # type: ignore[arg-type]
        bus.publish_nowait(evt)

    def _step_start(self, step_id: str) -> None:
        store.update_step(self.install_id, step_id, status="running", started_at=time.time())
        self._emit("step_start", step=step_id, status="running")

    def _step_end(self, step_id: str, success: bool, exit_code: int = 0, message: Optional[str] = None) -> None:
        status = "success" if success else "failed"
        store.update_step(
            self.install_id, step_id,
            status=status, finished_at=time.time(),
            exit_code=exit_code, message=message,
        )
        self._emit("step_end", step=step_id, status=status, payload={"exit_code": exit_code, "message": message})

    def _log(self, step_id: str, stream: str, line: str) -> None:
        self._emit("log", step=step_id, stream=stream, line=redact(line))  # type: ignore[arg-type]

    def _set_state(self, state: str) -> None:
        store.update_state(self.install_id, state)  # type: ignore[arg-type]
        self._emit("state", status=state)

    # ---------- subprocess plumbing ----------

    async def _run_bash(self, step_id: str, argv: list[str], cwd: Path, timeout: int,
                        extra_env: dict[str, str] | None = None) -> int:
        """Run a command under bash so UDP's shell scripts work cross-platform.

        *extra_env* is merged over the base subprocess env — used to point
        `docker compose` at the hardening overlay via COMPOSE_FILE for stacks
        whose start command doesn't pass explicit `-f`."""
        bash = _bash_executable()
        posix_cwd = _to_posix_path(cwd)
        quoted = " ".join(self._sh_quote(a) for a in argv)
        cmd_str = f"cd {self._sh_quote(posix_cwd)} && {quoted}"

        # Redact the echoed command in case argv contains a credential.
        self._log(step_id, "stdout", redact(f"$ {cmd_str}"))

        env = _build_subprocess_env()
        if extra_env:
            env.update(extra_env)

        try:
            proc = await asyncio.create_subprocess_exec(
                bash, "-c", cmd_str,  # no -l: don't source user profile
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=env,
            )
        except NotImplementedError:
            # Windows SelectorEventLoop (set by uvicorn --reload) does not support
            # asyncio subprocesses. Fall back to blocking subprocess in a thread.
            return await self._run_bash_threaded(step_id, bash, cmd_str, env, timeout)

        self._proc = proc

        async def _drain(stream: asyncio.StreamReader, kind: str) -> None:
            try:
                while True:
                    raw = await stream.readline()
                    if not raw:
                        return
                    try:
                        text = raw.decode("utf-8", errors="replace").rstrip("\r\n")
                    except Exception:
                        text = repr(raw)
                    self._log(step_id, kind, text)
            except asyncio.CancelledError:
                return

        drain_out = asyncio.create_task(_drain(proc.stdout, "stdout"))  # type: ignore[arg-type]
        drain_err = asyncio.create_task(_drain(proc.stderr, "stderr"))  # type: ignore[arg-type]

        timed_out = False
        try:
            await asyncio.wait_for(proc.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            timed_out = True
            self._log(step_id, "stderr", f"[timeout after {timeout}s; killing]")
            try:
                proc.kill()
            except ProcessLookupError:
                pass
            try:
                await asyncio.wait_for(proc.wait(), timeout=10)
            except asyncio.TimeoutError:
                pass
        finally:
            # Always drain to EOF, even on timeout or cancel.
            for t in (drain_out, drain_err):
                try:
                    await asyncio.wait_for(t, timeout=5)
                except asyncio.TimeoutError:
                    t.cancel()
                except Exception:
                    pass
            if self._proc is proc:
                self._proc = None

        if timed_out:
            return 124
        rc = proc.returncode
        return rc if rc is not None else 1

    async def _run_bash_threaded(
        self, step_id: str, bash: str, cmd_str: str, env: dict, timeout: int
    ) -> int:
        """Thread-based subprocess fallback for Windows SelectorEventLoop."""
        import subprocess as _sp
        import threading as _th
        import queue as _q

        loop = asyncio.get_event_loop()
        done_q: _q.Queue = _q.Queue()
        proc_box: list = [None]

        def _log_safe(stream: str, line: str) -> None:
            loop.call_soon_threadsafe(self._log, step_id, stream, line)

        def _run() -> None:
            try:
                p = _sp.Popen(
                    [bash, "-c", cmd_str],
                    stdout=_sp.PIPE, stderr=_sp.PIPE,
                    env=env,
                )
                proc_box[0] = p
                self._proc = p  # type: ignore[assignment]  # enables cancel()

                def _drain(pipe, kind: str) -> None:
                    for raw in pipe:
                        try:
                            line = raw.decode("utf-8", errors="replace").rstrip("\r\n")
                        except Exception:
                            line = repr(raw)
                        _log_safe(kind, line)

                t_out = _th.Thread(target=_drain, args=(p.stdout, "stdout"), daemon=True)
                t_err = _th.Thread(target=_drain, args=(p.stderr, "stderr"), daemon=True)
                t_out.start(); t_err.start()
                t_out.join(); t_err.join()
                p.wait()
                done_q.put(p.returncode if p.returncode is not None else 1)
            except Exception as exc:
                _log_safe("stderr", f"[subprocess error: {exc}]")
                done_q.put(1)

        _th.Thread(target=_run, daemon=True).start()

        deadline = loop.time() + timeout
        while True:
            if self._cancel:
                p = proc_box[0]
                if p:
                    try:
                        p.kill()
                    except Exception:
                        pass
                return 1
            try:
                rc = done_q.get_nowait()
                if self._proc is proc_box[0]:
                    self._proc = None
                return rc
            except _q.Empty:
                pass
            if loop.time() > deadline:
                self._log(step_id, "stderr", f"[timeout after {timeout}s; killing]")
                p = proc_box[0]
                if p:
                    try:
                        p.kill()
                    except Exception:
                        pass
                return 124
            await asyncio.sleep(0.25)

    @staticmethod
    def _sh_quote(s: str) -> str:
        if not s or any(c in s for c in " \t\"'\\$`!|&;()<>*?[]{}"):
            return "'" + s.replace("'", "'\\''") + "'"
        return s

    async def cancel(self) -> None:
        self._cancel = True
        if self._proc is not None:
            try:
                self._proc.kill()
            except ProcessLookupError:
                pass

    # ---------- pipeline steps ----------

    async def _step_prepare(self) -> bool:
        self._step_start("prepare")
        try:
            self.install_dir.parent.mkdir(parents=True, exist_ok=True)
            self._log("prepare", "stdout", f"workspace: {self.install_dir}")
            self._step_end("prepare", True)
            return True
        except Exception as e:
            self._step_end("prepare", False, message=str(e))
            return False

    async def _step_clone(self) -> bool:
        self._step_start("clone")
        repo = self.stack.repository
        url = repo.get("url")
        ref = repo.get("ref", "main")

        # Local bundled stack — copy from Studio's stacks/ directory
        local_src = repo.get("local_source")
        if local_src:
            import shutil as _shutil
            src_path = Path(__file__).parent.parent / local_src
            self._log("clone", "stdout", f"local source: {src_path}")
            if not src_path.exists():
                self._step_end("clone", False, exit_code=1, message=f"local source not found: {src_path}")
                return False
            self.install_dir.parent.mkdir(parents=True, exist_ok=True)
            if self.install_dir.exists():
                # Robustly remove the prior workspace. Handles reparse points
                # (Airflow scheduler/logs/latest) AND Windows reserved-name
                # files (a literal `nul`) that defeat both rmtree and rd /s /q.
                _force_remove_tree(self.install_dir)
            _shutil.copytree(str(src_path), str(self.install_dir))
            self._step_end("clone", True, exit_code=0)
            return True

        if (self.install_dir / ".git").exists():
            self._log("clone", "stdout", f"existing repo at {self.install_dir}, pulling latest")
            rc = await self._run_bash("clone", ["git", "fetch", "origin", ref], self.install_dir, timeout=120)
            if rc != 0:
                self._step_end("clone", False, exit_code=rc, message="git fetch failed")
                return False
            rc = await self._run_bash("clone", ["git", "checkout", ref], self.install_dir, timeout=60)
            if rc != 0:
                self._step_end("clone", False, exit_code=rc, message="git checkout failed")
                return False
            rc = await self._run_bash("clone", ["git", "reset", "--hard", f"origin/{ref}"], self.install_dir, timeout=60)
            ok = rc == 0
            self._step_end("clone", ok, exit_code=rc)
            return ok
        # Clone fresh into install_dir.parent then move; simpler: clone directly into install_dir
        self.install_dir.parent.mkdir(parents=True, exist_ok=True)
        rc = await self._run_bash(
            "clone",
            ["git", "clone", "--branch", ref, "--depth", "1", url, _to_posix_path(self.install_dir)],
            cwd=self.install_dir.parent,
            timeout=300,
        )
        ok = rc == 0
        self._step_end("clone", ok, exit_code=rc)
        return ok

    # Defaults for env vars referenced by UDP's compose but tied to optional
    # services (hms / ranger) we don't ship in the v0.3 cart. Setting them to
    # empty strings silences the "variable is not set" warnings; the services
    # themselves either get a docker-compose profile gate (in UDP) or fail
    # quietly without breaking the services we DO want.
    _SAFE_DEFAULTS = {
        "HMS_DB_NAME": "metastore",
        "HMS_DB_USER": "hive",
        "HMS_DB_PASSWORD": "hive",
        "RANGER_DB_NAME": "ranger",
        "RANGER_DB_USER": "ranger",
        "RANGER_DB_PASSWORD": "ranger",
    }

    def _patch_compose_images(self) -> None:
        """Rewrite the cloned UDP docker-compose.yml so it matches our cart:
          1. Update every `image: <repo>:<tag>` to the catalog's pinned tag
             (UDP upstream can drift; this keeps installs reproducible)
          2. Strip `depends_on` edges pointing at services that aren't in
             our cart (UDP includes enterprise services like hive-metastore
             and ranger that we don't ship; their dep edges would force
             docker compose to bring them up even when we don't ask for them)
        Idempotent — running twice is a no-op. Logs every change."""
        import re
        compose_path = self.install_dir / "docker-compose.yml"
        if not compose_path.exists():
            return
        text = compose_path.read_text(encoding="utf-8")
        original = text

        # ---- (1) image tag rewrites ----
        image_replacements: list[tuple[str, str]] = []
        for comp in self.stack.components:
            image = comp.get("image")
            if not image or ":" not in image:
                continue
            repo, _new_tag = image.rsplit(":", 1)
            pattern = re.compile(
                rf"^(\s*image:\s*){re.escape(repo)}:[^\s#]+",
                re.MULTILINE,
            )
            new_text, n = pattern.subn(rf"\g<1>{image}", text)
            if n:
                image_replacements.append((repo, image))
                text = new_text

        # ---- (2) prune depends_on entries for services not in our cart ----
        wanted_services = {c.get("service_name") for c in self.stack.components if c.get("service_name")}
        start_cmd = self.stack.data.get("commands", {}).get("start", {}) or {}
        wanted_services.update(start_cmd.get("extra_services") or [])

        dep_removals: list[str] = []
        # Match a single `<svc>:\n      condition: service_<state>\n` block inside a depends_on:
        dep_block_re = re.compile(
            r"^(?P<indent> {6,})(?P<svc>[a-z][a-z0-9_-]*):\n"
            r"\s+condition:\s*service_(?:healthy|started|completed_successfully)\s*\n",
            re.MULTILINE,
        )
        def _maybe_strip(m: re.Match) -> str:
            svc = m.group("svc")
            if svc in wanted_services or svc in ("create-bucket",):
                return m.group(0)
            dep_removals.append(svc)
            return ""
        text = dep_block_re.sub(_maybe_strip, text)

        # Remove `depends_on:` lines whose children were ALL pruned.
        # Lookahead matches: next line at same indent starting with any non-space
        # char (covers `<<:` merge keys, `command:`, etc.) OR top-level line.
        text = re.sub(
            r"^(?P<indent> {4,})depends_on:\s*\n(?=(?P=indent)\S|^[a-z])",
            "",
            text,
            flags=re.MULTILINE,
        )

        # Patch StarRocks FE startup with:
        #   - priority_networks (FE refuses leader election on Docker Desktop
        #     without it because the IP changes between restarts)
        #   - AWS_REGION / AWS_ENDPOINT_URL_S3 / etc env vars (empirically
        #     needed even on 3.3.12 — catalog property propagation doesn't
        #     fully cover the SDK default-credentials/region/endpoint chain
        #     when querying Iceberg-on-MinIO)
        # Same env vars also injected into BE in _patch_compose_be (below).
        fe_old = r"/opt/starrocks/fe/bin/start_fe.sh --daemon"
        fe_new = (
            r'echo "priority_networks = 172.16.0.0/12" >> /opt/starrocks/fe/conf/fe.conf'
            '\n        export AWS_REGION=us-east-1'
            '\n        export AWS_ACCESS_KEY_ID=admin'
            '\n        export AWS_SECRET_ACCESS_KEY=udp_admin_12345'
            '\n        export AWS_ENDPOINT_URL_S3=http://minio:9000'
            '\n        export AWS_S3_US_EAST_1_REGIONAL_ENDPOINT=regional'
            '\n        /opt/starrocks/fe/bin/start_fe.sh --daemon'
        )
        if fe_old in text and fe_new not in text:
            text = text.replace(fe_old, fe_new, 1)

        # Same env-var injection for BE (it needs the SDK config too for any
        # actual S3 read during query execution).
        be_old = r"/opt/starrocks/be/bin/start_be.sh --daemon"
        be_new = (
            r'echo "priority_networks = 172.16.0.0/12" >> /opt/starrocks/be/conf/be.conf'
            '\n        export AWS_REGION=us-east-1'
            '\n        export AWS_ACCESS_KEY_ID=admin'
            '\n        export AWS_SECRET_ACCESS_KEY=udp_admin_12345'
            '\n        export AWS_ENDPOINT_URL_S3=http://minio:9000'
            '\n        export AWS_S3_US_EAST_1_REGIONAL_ENDPOINT=regional'
            '\n        /opt/starrocks/be/bin/start_be.sh --daemon'
        )
        # Note: UDP's compose already has `echo "priority_networks..." >> be.conf`
        # before `start_be.sh --daemon`. To avoid double-prepending, match the
        # original line WITHOUT our priority_networks prefix.
        be_existing_re = re.compile(
            r'echo "priority_networks = 172\.16\.0\.0/12" >> /opt/starrocks/be/conf/be\.conf\s*\n\s*'
            r'/opt/starrocks/be/bin/start_be\.sh --daemon'
        )
        if be_existing_re.search(text):
            text = be_existing_re.sub(be_new, text, count=1)

        # Fix iceberg-rest healthcheck: CMD-SHELL uses /bin/sh (dash) which
        # does not support /dev/tcp. The tabulario image ships /usr/bin/bash,
        # so switch to CMD with explicit bash to make the probe actually work.
        text = re.sub(
            r'(healthcheck:\s*\n\s*test:\s*)\["CMD-SHELL",\s*"exec 3<>/dev/tcp/localhost/8181[^"]*"\]',
            r'\1["CMD", "bash", "-c", "exec 3<>/dev/tcp/localhost/8181"]',
            text,
        )

        # Downgrade `condition: service_healthy` → `condition: service_started`.
        # Several UDP images ship broken healthchecks (iceberg-rest's check
        # calls `wget` which isn't in the image, starrocks-fe takes minutes
        # to pass on first boot). Downgrading lets `docker compose up -d`
        # return after services START rather than waiting for healthchecks
        # that may never pass. The bootstrap step has its own wait-for
        # logic so we don't lose the readiness guarantee.
        text, healthy_to_started = re.subn(
            r"condition:\s*service_healthy",
            "condition: service_started",
            text,
        )

        # Windows-only: remap Spark's app-UI host port 4040 -> 18040. On
        # Windows, Hyper-V/WinNAT commonly reserves a large dynamic port block
        # (observed 3897-4602) that swallows 4040, so `docker compose up` dies
        # with "bind: An attempt was made to access a socket in a way forbidden
        # by its access permissions". 4040 is only the transient Spark job UI;
        # nothing in Studio's outputs or doctor references it, so bumping the
        # HOST side out of the reserved range is safe. Container stays 4040.
        spark_4040_remapped = 0
        if platform.system() == "Windows":
            text, spark_4040_remapped = re.subn(
                r'(-\s*")4040:4040(")',
                r'\g<1>18040:4040\g<2>',
                text,
            )

        if text != original:
            compose_path.write_text(text, encoding="utf-8")
            for repo, image in image_replacements:
                self._log("env", "stdout", f"compose image: {repo} -> {image}")
            if dep_removals:
                # Dedupe and report
                seen = []
                for d in dep_removals:
                    if d not in seen: seen.append(d)
                self._log("env", "stdout",
                          f"compose deps pruned (not in cart): {', '.join(seen)}")
            if healthy_to_started:
                self._log("env", "stdout",
                          f"compose: downgraded {healthy_to_started} 'service_healthy' deps to 'service_started' (UDP upstream healthchecks unreliable; bootstrap step has its own readiness gate)")
            if spark_4040_remapped:
                self._log("env", "stdout",
                          "compose: remapped Spark UI host port 4040 -> 18040 "
                          "(4040 falls in the Windows Hyper-V reserved port range)")

    def _patch_spark_defaults(self) -> None:
        """Repoint Spark's default `udp` catalog from hive-metastore to
        iceberg-REST. UDP's spark-defaults.conf configures `udp` for HMS
        and `udp_rest` for REST in parallel; we replace the HMS lines so
        the bootstrap job (hardcoded to use `udp`) runs against REST."""
        cfg = self.install_dir / "config" / "spark" / "spark-defaults.conf"
        if not cfg.exists():
            return
        text = cfg.read_text(encoding="utf-8")
        original = text
        # Replace the 3 hive-specific lines for the `udp` catalog
        replacements = [
            ("spark.sql.catalog.udp.type=hive", "spark.sql.catalog.udp.type=rest"),
            ("spark.sql.catalog.udp.uri=thrift://hive-metastore:9083",
             "spark.sql.catalog.udp.uri=http://iceberg-rest:8181"),
            ("spark.sql.catalog.udp.warehouse=s3a://datalake/warehouse",
             "spark.sql.catalog.udp.warehouse=s3://datalake/warehouse"),
        ]
        for old, new in replacements:
            text = text.replace(old, new)
        if text != original:
            cfg.write_text(text, encoding="utf-8")
            self._log("env", "stdout", "spark-defaults.conf: repointed 'udp' catalog from HMS to REST")

    def _write_stack_fragment(self, env: dict[str, str]) -> None:
        """v0.6.1 — write the per-stack docker-compose fragment, if any.

        UDP's upstream docker-compose.yml doesn't define `nessie`,
        `polaris`, `hive-metastore`, or `postgres-hms`. The four candidate
        stacks that rely on those services need a fragment dropped next
        to the base compose file so `docker compose up -d` actually has
        a definition to work from.

        Unlike `_write_optional_overlays`, this runs UNGATED on every
        install — but no-ops for stack ids without a registered renderer
        (e.g. the stable `udp-local-v0.2` cart). The fragment is INSERTED
        at the FRONT of self._overlays so its services are visible to any
        downstream opt-in overlay that might `depends_on` them.
        """
        try:
            from .stack_compose_fragments import (
                write_fragment,
                FRAGMENT_SERVICES,
            )
        except ImportError as e:
            self._log("env", "stderr",
                      f"stack fragment module unavailable: {e}")
            return
        try:
            path = write_fragment(self.stack.id, self.install_dir, env)
        except Exception as e:
            self._log("env", "stderr",
                      f"stack fragment write failed for '{self.stack.id}': "
                      f"{type(e).__name__}: {e} (continuing without it — "
                      f"the stack's `start` step will likely fail downstream)")
            return
        if path is None:
            # No fragment needed for this stack — stable path or unknown id.
            return
        services = list(FRAGMENT_SERVICES.get(self.stack.id, []) or [])
        # FRONT-insert so the fragment's services come up before any
        # opt-in overlay (Airflow/Dagster/Superset) that might depend on
        # them. The runner's docker_compose_up branch processes overlays
        # in order, appending `-f <file>` for each.
        self._overlays.insert(0, {
            "name": f"{self.stack.id}-fragment",
            "file": path,
            "services": services,
        })
        self._log("env", "stdout",
                  f"stack fragment for '{self.stack.id}' written: "
                  f"{path.name} ({len(services)} service"
                  f"{'s' if len(services) != 1 else ''})")

    def _effective_service_names(self) -> list[str]:
        """Ground-truth set of services `docker compose up` will touch: parse the
        base cloned compose PLUS every registered overlay/fragment FILE and union
        their service keys. Parsing the actual files (not each overlay's declared
        `services` metadata) means a service defined in an overlay file is hardened
        even if the writer forgot to list it — hardening coverage never silently
        lags the real compose model. Read-only (keys only) — never re-serializes a
        file, so the fragile StarRocks command heredocs are left untouched.

        Falls back to an overlay's declared `services` only when its file can't be
        parsed, and emits a prominent SECURITY warning if the BASE compose can't be
        parsed (that would leave the bulk of the stack unhardened)."""
        import yaml

        def _service_keys(path: Path) -> list[str] | None:
            try:
                doc = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
                return list((doc.get("services") or {}).keys())
            except Exception:
                return None

        names: list[str] = []
        base = self.install_dir / "docker-compose.yml"
        if base.exists():
            keys = _service_keys(base)
            if keys is None:
                self._log("env", "stderr",
                          "SECURITY: harden could not parse base docker-compose.yml; "
                          "the runtime hardening overlay will cover overlay services "
                          "only — base services may run UNHARDENED")
            else:
                names.extend(keys)
        for ov in self._overlays:
            f = ov.get("file")
            keys = _service_keys(f) if f else None
            # Trust the file's actual service keys; fall back to declared metadata
            # only when the file is missing/unparseable.
            names.extend(keys if keys is not None else (ov.get("services") or []))
        return list(dict.fromkeys(n for n in names if n))

    def _write_harden_overlay(self, env: dict[str, str]) -> None:
        """P0.2 — write docker-compose.harden.yml and register it LAST so its
        runtime security options layer over the base compose + fragment + any
        opt-in overlays.

        Default ON: security_opt no-new-privileges on every service (safe for
        all certified stacks). Disable with LHS_HARDEN_RUNTIME_DISABLED=1.
        Strict cap-drop/pids-limit is opt-in via LHS_HARDEN_STRICT (needs
        per-stack verification before it can be trusted)."""
        if (_is_truthy(env.get(compose_hardening.DISABLE_ENV))
                or _is_truthy(os.environ.get(compose_hardening.DISABLE_ENV))):
            self._log("env", "stdout",
                      f"runtime hardening disabled via "
                      f"{compose_hardening.DISABLE_ENV}")
            return
        names = self._effective_service_names()
        if not names:
            self._log("env", "stderr",
                      "harden: no services resolved; skipping hardening overlay")
            return
        strict = (_is_truthy(env.get(compose_hardening.STRICT_ENV))
                  or _is_truthy(os.environ.get(compose_hardening.STRICT_ENV)))
        doc = compose_hardening.build_harden_overlay(names, strict=strict)
        import yaml
        path = self.install_dir / compose_hardening.OVERLAY_FILENAME
        path.write_text(
            yaml.dump(doc, default_flow_style=False, sort_keys=False),
            encoding="utf-8",
        )
        # Register with NO services: the overlay only modifies services that are
        # already started by earlier -f files, it must not add to the `up -d`
        # service list. Drop any pre-existing 'harden' entry first so re-runs
        # never produce duplicate `-f docker-compose.harden.yml` flags.
        self._overlays = [o for o in self._overlays if o.get("name") != "harden"]
        self._overlays.append({
            "name": "harden",
            "file": path,
            "services": [],
        })
        self._log("env", "stdout",
                  f"runtime hardening overlay written: {path.name} "
                  f"({len(names)} service{'s' if len(names) != 1 else ''}, "
                  f"strict={strict})")

    def _write_optional_overlays(self, env: dict[str, str]) -> None:
        """v0.6.1 — write opt-in compose overlays (Airflow / Dagster / Superset)
        next to the base compose file.

        Each overlay module is gated by an env flag (LHS_*_ENABLED). When
        the flag is on, the writer drops a docker-compose.<name>.yml into
        install_dir + populates self._overlays so _step_cmd injects
        `-f <overlay>.yml` and appends the overlay's services to the
        `docker compose up -d <services>` argv.

        Default behavior is unchanged: no flag → no overlay written → no
        change to the existing stable install path.

        Failures here are NON-FATAL — overlays are operational extras,
        not part of the certified-stack contract. A broken overlay should
        never block the base stack from coming up.
        """
        try:
            from . import airflow_overlay, dagster_overlay, superset_overlay, observability_overlay
        except ImportError as e:
            self._log("env", "stderr", f"overlay modules unavailable: {e}")
            return

        for mod in (airflow_overlay, dagster_overlay, superset_overlay, observability_overlay):
            flag = getattr(mod, "ENV_FLAG", None)
            if not flag:
                continue
            # Honor both the merged env dict (manifest defaults + user
            # overrides) and the parent process env, so operators can
            # opt in via `LHS_AIRFLOW_ENABLED=true` before invoking Studio.
            enabled = (
                _is_truthy(env.get(flag))
                or _is_truthy(os.environ.get(flag))
            )
            if not enabled:
                continue
            name = mod.__name__.rsplit(".", 1)[-1].replace("_overlay", "")
            try:
                path = mod.write_airflow_overlay(self.install_dir, env) \
                    if mod is airflow_overlay else (
                        mod.write_dagster_overlay(self.install_dir, env)
                        if mod is dagster_overlay else (
                            mod.write_superset_overlay(self.install_dir, env)
                            if mod is superset_overlay else
                            mod.write_observability_overlay(self.install_dir, env)
                        )
                    )
            except Exception as e:
                self._log("env", "stderr",
                          f"overlay '{name}' write failed: {type(e).__name__}: {e} "
                          f"(continuing without it)")
                continue
            if path is None:
                # Writer chose to no-op (e.g. validation failed inside).
                self._log("env", "stdout",
                          f"overlay '{name}' enabled but writer returned no path; skipping")
                continue
            services = list(getattr(mod, "SERVICES", []) or [])
            self._overlays.append({
                "name": name,
                "file": path,
                "services": services,
            })
            self._log("env", "stdout",
                      f"overlay '{name}' enabled: {path.name} (services: {', '.join(services) or '(none declared)'})")

    def _write_studio_bootstrap(self) -> None:
        """Drop Studio-owned bootstrap + smoke scripts into the install dir's
        scripts/ directory. Replace UDP's equivalents which hard-require
        hive-metastore. Studio's scripts use ONLY the services we ship.

        The pair written is selected from _STUDIO_SCRIPT_SETS by stack.id —
        each certified stack registers a (bootstrap_name, smoke_name) pair
        whose filenames match what the stack manifest's `commands.bootstrap`
        and `commands.smoke` argv reference. Unknown stack ids skip the
        write silently; the manifest may run UDP's native scripts instead.
        """
        # Derive the table format(s) the user picked in the cart so the smoke's
        # ETL verification runs THAT format (iceberg/delta/hudi). The base stack
        # is the same regardless; only the format written/verified changes. The
        # runner substitutes __ETLV_FORMATS__ in the smoke body.
        etlv_formats = self._chosen_table_formats()

        # Stack-specific container names / endpoints for the shared ETL block.
        # DEFAULT = udp-family values (byte-identical to before); a stack in
        # _SMOKE_SUBST overrides only what differs.
        subst = dict(_SMOKE_SUBST_DEFAULT)
        subst.update(_SMOKE_SUBST.get(self.stack.id, {}))

        def _apply_subst(body: str) -> str:
            body = body.replace("__ETLV_FORMATS__", etlv_formats)
            for ph, val in subst.items():
                body = body.replace(ph, val)
            return body

        scripts_dir = self.install_dir / "scripts"

        # Additive 3-catalog feature for stacks that own their bootstrap/smoke
        # (e.g. Streaming): drop a standalone lhs-etl-verify.sh that their smoke
        # calls at the end. Runs even when the stack has no _STUDIO_SCRIPT_SETS
        # entry, so the Kafka/Flink build is left completely untouched.
        if self.stack.id in _ETL_FEATURE_STACKS:
            scripts_dir.mkdir(parents=True, exist_ok=True)
            etl_sh = (
                "#!/usr/bin/env bash\n"
                "# Studio-generated: additive 3-catalog (iceberg/hudi/delta) verification.\n"
                "# Source of truth: backend/etl_verify_job.py + _ETL_MULTIFORMAT_BLOCK.\n"
                "set -eo pipefail\n"
                "export MSYS_NO_PATHCONV=1\n"
                "export MSYS2_ARG_CONV_EXCL='*'\n\n"
                + _apply_subst(_ETL_MULTIFORMAT_BLOCK)
                + '\necho "[studio-etl-verify] done"\n'
            )
            p = scripts_dir / "lhs-etl-verify.sh"
            p.write_text(etl_sh, encoding="utf-8")
            try:
                p.chmod(0o755)
            except Exception:
                pass

        script_set = _STUDIO_SCRIPT_SETS.get(self.stack.id)
        if script_set is None:
            if self.stack.id not in _ETL_FEATURE_STACKS:
                self._log("env", "stdout",
                          f"no studio script set for stack '{self.stack.id}' — "
                          "falling back to whatever the manifest points at")
            return

        scripts_dir.mkdir(parents=True, exist_ok=True)
        for name, body in script_set:
            body = _apply_subst(body)
            path = scripts_dir / name
            path.write_text(body, encoding="utf-8")
            try:
                path.chmod(0o755)
            except Exception:
                pass

    def _chosen_table_formats(self) -> str:
        """Table format(s) from the install's cart → ETLV_FORMATS value.
        Defaults to 'iceberg' when the cart has no explicit format (the base
        stack is Iceberg-oriented). Multiple picks are preserved, comma-joined."""
        try:
            rec = store.get(self.install_id)
            cart = list((rec.cart if rec else []) or [])
        except Exception:
            cart = []
        picked = []
        if "iceberg" in cart:
            picked.append("iceberg")
        if "delta" in cart or "spark-delta" in cart:
            picked.append("delta")
        if "hudi" in cart or "hudi-v1" in cart:
            picked.append("hudi")
        return ",".join(picked) if picked else "iceberg"

    async def _step_env(self, overrides: dict[str, str]) -> bool:
        self._step_start("env")
        env_path = self.install_dir / ".env"

        # ---- patch the cloned UDP repo with our catalog's pinned image versions ----
        # UDP's docker-compose.yml may carry stale image tags upstream
        # (caught in pilot: tabulario/spark-iceberg:3.5.1_1.5.2 was removed
        # from Docker Hub). We override every image to whatever the catalog
        # currently certifies, so an out-of-date UDP clone still installs
        # cleanly. Bonus: this is what makes the cart's component versions
        # actually mean something (closes part of the Gemini "guided
        # illusion" gap for image tags specifically).
        try:
            self._patch_compose_images()
        except Exception as e:
            self._log("env", "stderr", f"compose image patch warning: {e}")

        # Sanitize user overrides; reject anything dangerous outright.
        clean_overrides, rejections = sanitize_env_overrides(overrides)
        for r in rejections:
            self._log("env", "stderr", f"rejected override {r}")
        if rejections and not clean_overrides:
            # If everything was rejected and nothing came through, still proceed
            # with defaults — but tell the user.
            pass

        # Defaults are trusted (from the manifest), but quote them too for safety.
        # _SAFE_DEFAULTS supplies dummy values for env vars referenced by
        # optional UDP services we don't ship (hms/ranger) — silences
        # docker-compose's "variable not set" warnings on every command.
        merged: dict[str, str] = {**self._SAFE_DEFAULTS, **self.stack.env_defaults, **clean_overrides}

        # Name the compose default network with a HYPHEN (not the default
        # `<project>_default` underscore). A container's reverse-DNS PTR is
        # `<container>.<network>`; an underscore there is an illegal URI hostname
        # char and breaks HMS self-resolution -> StarRocks getAllDatabases for
        # the Delta/Hudi catalogs. Install-specific so side-by-side installs
        # don't share a network. Fragments reference this via ${LHS_NET}.
        merged.setdefault("LHS_NET", f"{self.install_dir.name.replace('_', '-')}-net")

        # Auto-generate Airflow secrets if not already in merged/overrides.
        # Airflow refuses to start with blank FERNET_KEY or SECRET_KEY.
        if "AIRFLOW_FERNET_KEY" not in merged or not merged["AIRFLOW_FERNET_KEY"]:
            import base64 as _b64, os as _os
            merged["AIRFLOW_FERNET_KEY"] = _b64.urlsafe_b64encode(_os.urandom(32)).decode()
        if "AIRFLOW_WEBSERVER_SECRET_KEY" not in merged or not merged["AIRFLOW_WEBSERVER_SECRET_KEY"]:
            import secrets as _sec
            merged["AIRFLOW_WEBSERVER_SECRET_KEY"] = _sec.token_hex(32)

        # P0.4b — opt-in per-install credential generation (default OFF). When
        # LHS_GENERATE_CREDENTIALS is set, replace the shipped demo MinIO secret
        # with a strong random one so no two installs share the public default.
        # Default path is byte-identical: without the flag nothing is generated
        # and every consumer resolves to ${MINIO_ROOT_PASSWORD:-udp_admin_12345}.
        # An explicit operator override always wins over generation.
        self._generated_minio_secret: str | None = None
        gen_creds = (_is_truthy(merged.get(credential_gen.GENERATE_ENV))
                     or _is_truthy(os.environ.get(credential_gen.GENERATE_ENV)))
        if gen_creds and not clean_overrides.get(credential_gen.MINIO_SECRET_ENV):
            self._generated_minio_secret = credential_gen.generate_secret()
            merged[credential_gen.MINIO_SECRET_ENV] = self._generated_minio_secret
            self._log("env", "stdout",
                      "P0.4b: generated a per-install MinIO secret "
                      f"({credential_gen.MINIO_SECRET_ENV}=********)")

        # Patch Spark's catalog config: swap the default `udp` catalog from
        # hive-metastore-backed to iceberg-REST-backed so the Spark bootstrap
        # job works without hive-metastore. UDP ships a parallel `udp_rest`
        # catalog already configured for REST — we redirect `udp` at the same
        # endpoint so the bootstrap job (which hardcodes catalog name `udp`)
        # runs unmodified.
        try:
            self._patch_spark_defaults()
        except Exception as e:
            self._log("env", "stderr", f"spark-defaults patch warning: {e}")

        # Write Studio's own bootstrap script that uses REST catalog only.
        # The manifest's `bootstrap` command points at this script via
        # `./scripts/lhs-bootstrap.sh`.
        try:
            self._write_studio_bootstrap()
        except Exception as e:
            self._log("env", "stderr", f"studio bootstrap write warning: {e}")

        # v0.6.1 — write opt-in compose overlays (Airflow / Dagster / Superset)
        # if their env flags are set. Default: no flag → no overlay → no change.
        try:
            self._write_optional_overlays(merged)
        except Exception as e:
            self._log("env", "stderr", f"overlay write warning: {e}")

        # v0.6.1 — write the per-stack compose fragment (required for the
        # four candidate stacks whose catalog/HMS/Polaris services aren't
        # in UDP's upstream compose). No-ops for the stable udp-local-v0.2
        # stack and any other id without a registered renderer.
        try:
            self._write_stack_fragment(merged)
        except Exception as e:
            self._log("env", "stderr", f"stack fragment write warning: {e}")

        # P0.2 — write the runtime hardening overlay LAST, after fragment +
        # optional overlays are registered, so it can enumerate and harden
        # every service that will actually start. Non-fatal: a hardening write
        # failure must never block the certified stack from coming up.
        try:
            self._write_harden_overlay(merged)
        except Exception as e:
            self._log("env", "stderr", f"harden overlay write warning: {e}")

        # Make UDP scripts executable. On Windows chmod is a near-noop, but on
        # Linux/macOS it matters. Don't swallow surprising errors silently.
        try:
            for name in ("udp",):
                p = self.install_dir / name
                if p.exists():
                    p.chmod(p.stat().st_mode | 0o111)
            scripts_dir = self.install_dir / "scripts"
            if scripts_dir.is_dir():
                for p in scripts_dir.glob("*.sh"):
                    p.chmod(p.stat().st_mode | 0o111)
        except Exception as e:
            self._log("env", "stderr", f"chmod warning: {e}")

        try:
            lines = [f"{k}={quote_env_value(v)}" for k, v in merged.items()]
            env_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
            try:
                env_path.chmod(0o600)
            except Exception:
                pass
            # P0.4b — with a generated secret in hand, sweep the install dir and
            # replace the shipped demo literal everywhere it was written (bootstrap
            # + smoke scripts, patched compose, StarRocks conf injection, configs).
            # Runs AFTER every env-step writer, so it catches all of them at once.
            if self._generated_minio_secret:
                self._rotate_install_credential(
                    credential_gen.DEMO_MINIO_SECRET,
                    self._generated_minio_secret,
                )
            # Echo redacted preview line-by-line.
            for k, v in merged.items():
                is_secret = (
                    k in SECRET_KEYS
                    or "PASSWORD" in k.upper()
                    or "SECRET" in k.upper()
                    or "TOKEN" in k.upper()
                )
                shown = ("********" if v else "(empty)") if is_secret else v
                self._log("env", "stdout", f"{k}={shown}")
            self._step_end("env", True)
            return True
        except Exception as e:
            self._step_end("env", False, message=str(e))
            return False

    def _reconstruct_overlays_from_disk(self) -> None:
        """Rebuild self._overlays from compose files a PRIOR env step wrote.

        Used by the docker_compose_up branch when self._overlays is empty —
        which happens on Retry/Skip because those restart the pipeline after
        the env step, so the in-memory overlay list is never populated in the
        new runner instance. The files themselves persist in install_dir, so
        we rediscover them here and re-attach their `-f` flags + services.

        The per-stack REQUIRED fragment goes FIRST (front-inserted) so its
        services come up before any opt-in overlay that might depend on them,
        matching _write_stack_fragment's ordering.
        """
        # Per-stack required fragment (nessie / hms / delta / polaris / trino /
        # superset / trino+jupyter). Front of the list.
        try:
            from .stack_compose_fragments import FRAGMENT_FILENAME, FRAGMENT_SERVICES
            frag = self.install_dir / FRAGMENT_FILENAME
            if frag.exists():
                self._overlays.insert(0, {
                    "name": f"{self.stack.id}-fragment",
                    "file": frag,
                    "services": list(FRAGMENT_SERVICES.get(self.stack.id, []) or []),
                })
                self._log("start", "stdout",
                          f"recovered stack fragment from disk: {frag.name} "
                          "(env step didn't re-run — likely a retry)")
        except ImportError:
            pass
        # Opt-in overlays (airflow / dagster / superset / observability). A file
        # only exists on disk if its LHS_*_ENABLED flag was on at env time, so
        # re-including it here preserves the operator's original choice.
        try:
            from . import airflow_overlay, dagster_overlay, superset_overlay, observability_overlay
            for mod in (airflow_overlay, dagster_overlay, superset_overlay, observability_overlay):
                fname = getattr(mod, "OVERLAY_FILENAME", None)
                if not fname:
                    continue
                f = self.install_dir / fname
                if f.exists():
                    name = mod.__name__.rsplit(".", 1)[-1].replace("_overlay", "")
                    self._overlays.append({
                        "name": name,
                        "file": f,
                        "services": list(getattr(mod, "SERVICES", []) or []),
                    })
        except ImportError:
            pass
        # P0.2 runtime hardening overlay — LAST, so it layers over everything.
        # services:[] because it only modifies services started by other files.
        # Honor a now-set disable flag: a stale overlay file from a prior run
        # must NOT silently re-harden when the operator has since disabled it.
        # Dedupe first so a retry never doubles the `-f` flag.
        self._overlays = [o for o in self._overlays if o.get("name") != "harden"]
        harden = self.install_dir / compose_hardening.OVERLAY_FILENAME
        disabled = _is_truthy(os.environ.get(compose_hardening.DISABLE_ENV))
        if harden.exists() and not disabled:
            self._overlays.append({
                "name": "harden",
                "file": harden,
                "services": [],
            })
            self._log("start", "stdout",
                      f"recovered hardening overlay from disk: {harden.name}")

    async def _step_cmd(self, step_id: str, cmd_name: str) -> bool:
        self._step_start(step_id)
        try:
            spec = self.stack.command(cmd_name)
        except KeyError as e:
            self._step_end(step_id, False, message=str(e))
            return False

        # Special command type: docker_compose_up with explicit service list
        # built from the stack's components. Lets us skip enterprise services
        # that UDP's compose includes by default (hive-metastore, ranger).
        if spec.get("type") == "docker_compose_up":
            services: list[str] = []
            for comp in self.stack.components:
                sn = comp.get("service_name")
                if sn:
                    services.append(sn)
            services.extend(spec.get("extra_services") or [])
            if not services:
                self._step_end(step_id, False, message="no services to start (cart empty?)")
                return False
            # Retry/Skip self-heal: those paths restart the pipeline at a
            # LATER step, so `_step_env` (which populates self._overlays)
            # never ran in THIS runner instance — leaving self._overlays
            # empty even though the fragment / overlay compose files are
            # already on disk from the original env step. Without them the
            # `-f <fragment>.yml` flag is omitted and docker compose fails
            # with `no such service: <fragment-service>` (e.g. trino/nessie).
            # Rebuild from disk so `start` is idempotent across retries.
            if not self._overlays:
                self._reconstruct_overlays_from_disk()

            # v0.6.1 — inject opt-in overlay compose files via `-f` and
            # extend the explicit service list with the overlay's services.
            # When _overlays is empty (default — no LHS_*_ENABLED flags and
            # no per-stack fragment), this is a no-op and the argv matches
            # the pre-v0.6.1 shape exactly. Order: `-f base -f overlay up -d`.
            argv = ["docker", "compose"]
            if self._overlays:
                # The base compose file is the default discovery target;
                # we have to name it explicitly so `-f overlay.yml` doesn't
                # REPLACE it.
                base_compose = self.install_dir / "docker-compose.yml"
                if base_compose.exists():
                    argv += ["-f", base_compose.name]
                for ov in self._overlays:
                    argv += ["-f", ov["file"].name]
                    for svc in ov.get("services", []) or []:
                        if svc and svc not in services:
                            services.append(svc)
                self._log(step_id, "stdout",
                          f"compose: using {len(self._overlays)} overlay(s): "
                          + ", ".join(ov["name"] for ov in self._overlays))
            argv += ["up", "-d"] + services
        else:
            argv = list(spec["argv"])

        # P0.2 — raw-argv start commands (enterprise-hadoop / streaming /
        # techsophy run `docker compose up` with NO explicit -f, so they don't
        # inherit the hardening overlay the docker_compose_up branch injects.
        # Point compose at base + harden via COMPOSE_FILE for the start step.
        # Compose ignores COMPOSE_FILE when explicit -f is passed (the
        # docker_compose_up branch), so this is safe for every stack.
        extra_env: dict[str, str] | None = None
        if step_id == "start":
            harden = self.install_dir / compose_hardening.OVERLAY_FILENAME
            base_name = self._base_compose_filename()
            if harden.exists() and base_name:
                extra_env = {
                    "COMPOSE_FILE": base_name + os.pathsep + harden.name,
                }
                self._log(step_id, "stdout",
                          f"compose: COMPOSE_FILE={extra_env['COMPOSE_FILE']} "
                          f"(runtime hardening overlay applied)")

        rc = await self._run_bash(step_id, argv, self.install_dir,
                                  int(spec.get("timeout", 600)), extra_env=extra_env)
        ok = rc == 0
        self._step_end(step_id, ok, exit_code=rc)
        return ok

    def _rotate_install_credential(self, old: str, new: str) -> None:
        """P0.4b — replace the unique demo secret *old* with *new* across every
        text artifact in install_dir (bootstrap/smoke scripts, patched compose,
        StarRocks conf injection, generated configs, .env defaults).

        A plain string replace of an UNAMBIGUOUS literal — no YAML round-trip, so
        the fragile StarRocks command heredocs are preserved. Binary and .git
        files are skipped. Idempotent: files without *old* are untouched."""
        if not old or old == new:
            return
        changed = 0
        for root, dirs, files in os.walk(self.install_dir):
            if ".git" in dirs:
                dirs.remove(".git")
            for fn in files:
                p = Path(root) / fn
                try:
                    text = p.read_text(encoding="utf-8")
                except (UnicodeDecodeError, OSError):
                    continue  # binary or unreadable — not a credential carrier
                if old in text:
                    try:
                        p.write_text(text.replace(old, new), encoding="utf-8")
                        changed += 1
                    except OSError as e:
                        self._log("env", "stderr",
                                  f"P0.4b: could not rewrite {p.name}: {e}")
        self._log("env", "stdout",
                  f"P0.4b: rotated demo MinIO secret across {changed} install file(s)")

    def _base_compose_filename(self) -> str | None:
        """Name of the stack's base compose file in install_dir, if present.
        Compose's standard discovery order — used to build COMPOSE_FILE so the
        hardening overlay merges over the right base."""
        for name in ("docker-compose.yml", "docker-compose.yaml",
                     "compose.yml", "compose.yaml"):
            if (self.install_dir / name).exists():
                return name
        return None

    async def _step_finalize(self) -> bool:
        self._step_start("finalize")
        urls = self.stack.output_urls(self.host)
        conns = self.stack.output_connections(self.host)
        outputs = {"urls": urls, "connections": conns}
        store.set_outputs(self.install_id, outputs)
        self._emit("result", payload=outputs)
        # Capture evidence: result.json, system-info.json, full-log.txt
        evidence_ok = True
        try:
            from .evidence import capture
            rec = store.get(self.install_id)
            if rec:
                out_dir = capture(rec)
                outputs["evidence_dir"] = str(out_dir)
                store.set_outputs(self.install_id, outputs)
                self._log("finalize", "stdout", f"evidence captured: {out_dir}")
        except Exception as e:
            evidence_ok = False
            self._log("finalize", "stderr", f"evidence capture failed: {e}")
        # Step is success only if evidence wrote cleanly; stack is still READY either way.
        self._step_end("finalize", evidence_ok,
                       message=None if evidence_ok else "evidence capture failed (stack is still READY)")
        return evidence_ok

    # ---------- top-level orchestration ----------

    # Ordered sequence: (step_id, state_to_enter_before_running, callable_factory).
    # Each callable_factory takes the runner + overrides and returns a coroutine.
    _PIPELINE: list[tuple[str, str]] = [
        ("prepare",   "CLONING_REPO"),
        ("clone",     "CLONING_REPO"),
        ("env",       "WRITING_ENV"),
        ("doctor",    "RUNNING_DOCTOR"),
        ("start",     "STARTING_STACK"),
        ("bootstrap", "BOOTSTRAPPING"),
        ("smoke",     "SMOKE_TESTING"),
        ("finalize",  "READY"),
    ]

    # Steps the user is allowed to Skip (the install can still complete).
    SKIPPABLE = frozenset({"smoke", "finalize"})

    async def _execute_step(self, step_id: str, env_overrides: dict[str, str]) -> bool:
        """Dispatch a single step. Used by both initial run and retry."""
        if step_id == "prepare":   return await self._step_prepare()
        if step_id == "clone":     return await self._step_clone()
        if step_id == "env":       return await self._step_env(env_overrides)
        if step_id == "doctor":    return await self._step_cmd("doctor", "doctor")
        if step_id == "start":     return await self._step_cmd("start", "start")
        if step_id == "bootstrap": return await self._step_cmd("bootstrap", "bootstrap")
        if step_id == "smoke":     return await self._step_cmd("smoke", "smoke")
        if step_id == "finalize":  return await self._step_finalize()
        raise ValueError(f"unknown step: {step_id}")

    def _step_index(self, step_id: str) -> int:
        for i, (sid, _) in enumerate(self._PIPELINE):
            if sid == step_id: return i
        return -1

    async def run(
        self,
        env_overrides: dict[str, str],
        *,
        start_at: str = "prepare",
        post_env_hook=None,
    ) -> None:
        """Run the pipeline starting at `start_at` (default = beginning).

        On the first run this drives all steps. On a Retry, the caller passes
        the failed step id as start_at; on Skip, the caller passes the NEXT
        step id; rollback runs ./udp clean instead.

        post_env_hook: optional async coroutine called after the `env` step
        succeeds but before `doctor`/`start`. Signature: hook(install_dir).
        Used by the AI provisioner to inject AI-generated configs.
        """
        try:
            self._set_state("INSPECTING")  # caller did the inspection already
            self._set_state("READY_TO_INSTALL")

            start_idx = self._step_index(start_at)
            if start_idx < 0:
                return self._fail(f"unknown start step: {start_at}")

            for step_id, state in self._PIPELINE[start_idx:]:
                if self._cancel:
                    return self._fail("cancelled")
                # Don't downgrade state — but READY is the terminal of finalize
                if state != "READY":
                    self._set_state(state)
                ok = await self._execute_step(step_id, env_overrides)
                if ok and step_id == "env" and post_env_hook is not None:
                    try:
                        await post_env_hook(self.install_dir)
                    except Exception as _hook_exc:
                        self._log("env", "stderr", f"post-env hook warning: {_hook_exc}")
                if not ok:
                    # finalize failing means evidence didn't write, stack is still up
                    if step_id == "finalize":
                        self._set_state("READY")
                        self._emit("state", status="READY")
                        try:
                            await notify(
                                self.install_id,
                                "install_completed",
                                "info",
                                f"Install ready: {self.stack.id}",
                                self._completion_body(),
                                links={"success": f"/installs/{self.install_id}"},
                            )
                        except Exception:
                            pass  # never let notifications break the install
                        return
                    # Smoke-specific notification before the FAILED transition
                    if step_id == "smoke":
                        try:
                            await notify(
                                self.install_id,
                                "smoke_failed",
                                "warn",
                                f"Smoke test failed: {self.stack.id}",
                                self._step_error_tail("smoke"),
                            )
                        except Exception:
                            pass  # never let notifications break the install
                    return self._fail(f"{step_id} failed")

            self._set_state("READY")
            self._emit("state", status="READY")
            try:
                await notify(
                    self.install_id,
                    "install_completed",
                    "info",
                    f"Install ready: {self.stack.id}",
                    self._completion_body(),
                    links={"success": f"/installs/{self.install_id}"},
                )
            except Exception:
                pass  # never let notifications break the install
        except asyncio.CancelledError:
            self._fail("cancelled")
        except Exception as e:
            import traceback as _tb
            self._fail(f"unexpected: {type(e).__name__}: {e} | {_tb.format_exc()}")

    def _fail(self, msg: str) -> None:
        store.update_state(self.install_id, "FAILED", error=msg)
        self._emit("state", status="FAILED", payload={"error": msg})
        self._emit("error", line=msg)
        # Fire-and-forget notification — never let dispatcher errors break the install.
        try:
            failing_step = self._current_failing_step() or "unknown"
            asyncio.create_task(notify(
                self.install_id,
                "install_failed",
                "critical",
                f"Install failed at {failing_step}",
                self._step_error_tail(failing_step) or msg,
                links={"diagnose": f"/api/installs/{self.install_id}/diagnose"},
            ))
        except Exception:
            pass  # never let notifications break the install

    # ---------- notification body helpers ----------

    def _completion_body(self) -> str:
        try:
            urls = self.stack.output_urls(self.host)
        except Exception:
            urls = {}
        lines = [f"install_dir: {self.install_dir}"]
        if urls:
            lines.append("services:")
            for name, url in urls.items():
                lines.append(f"  {name}: {url}")
        return "\n".join(lines)

    def _step_error_tail(self, step_id: str, max_chars: int = 800) -> str:
        try:
            rec = store.get(self.install_id)
            if not rec:
                return ""
            for s in rec.steps:
                if s.id == step_id and s.message:
                    msg = s.message
                    return msg if len(msg) <= max_chars else msg[-max_chars:]
        except Exception:
            return ""
        return ""

    def _current_failing_step(self) -> Optional[str]:
        try:
            rec = store.get(self.install_id)
            if not rec:
                return None
            for s in rec.steps:
                if s.status == "failed":
                    return s.id
        except Exception:
            return None
        return None


class RemoteClusterRunner:
    """Pipeline for mode=remote-cluster stacks.

    No Docker install pipeline. Two steps only:
      1. verify  — HTTP reachability probe for each component's first URL
      2. finalize — capture outputs (URLs + connections) into the store
    """

    _PIPELINE: list[tuple[str, str]] = [
        ("verify",   "STARTING_STACK"),
        ("finalize", "READY"),
    ]
    SKIPPABLE: frozenset[str] = frozenset({"verify"})

    def __init__(self, stack: StackManifest, install_id: str, host: str, install_dir: Path):
        self.stack = stack
        self.install_id = install_id
        self.host = host
        self.install_dir = install_dir
        self._cancel = False

    def _emit(self, kind: str, **kwargs) -> None:
        from .events import bus
        bus.emit(self.install_id, {"type": kind, **kwargs})

    def _step_start(self, step_id: str) -> None:
        store.step_start(self.install_id, step_id)
        self._emit("step_start", step=step_id)

    def _step_end(self, step_id: str, success: bool, exit_code: int = 0, message: Optional[str] = None) -> None:
        store.step_end(self.install_id, step_id, success, exit_code=exit_code, message=message)
        self._emit("step_end", step=step_id, success=success, exit_code=exit_code, message=message)

    def _log(self, step_id: str, stream: str, line: str) -> None:
        store.append_log(self.install_id, step_id, stream, line)
        self._emit("log", step=step_id, stream=stream, line=line)

    def _set_state(self, state: str) -> None:
        store.update_state(self.install_id, state)
        self._emit("state", status=state)

    def _fail(self, msg: str) -> None:
        store.update_state(self.install_id, "FAILED", error=msg)
        self._emit("state", status="FAILED", payload={"error": msg})
        self._emit("error", line=msg)

    def _step_index(self, step_id: str) -> int:
        for i, (sid, _) in enumerate(self._PIPELINE):
            if sid == step_id:
                return i
        return -1

    async def _step_verify(self) -> bool:
        """Probe each component's first port via TCP to confirm cluster is reachable."""
        import socket as _socket
        self._step_start("verify")
        passed = 0
        warned = 0
        for comp in self.stack.components:
            comp_id = comp.get("id", "?")
            hostname = comp.get("host", "")
            ports = comp.get("ports", [])
            if not hostname or not ports:
                continue
            port = int(ports[0])
            try:
                with _socket.create_connection((hostname, port), timeout=6):
                    pass
                self._log("verify", "stdout", f"  ✓ {comp_id}  {hostname}:{port}")
                passed += 1
            except OSError as e:
                self._log("verify", "stdout", f"  ~ {comp_id}  {hostname}:{port}  ({e})")
                warned += 1

        msg = f"{passed} component(s) reachable, {warned} unreachable (VPN/firewall may apply)"
        self._log("verify", "stdout", msg)
        # We treat any partial reachability as a pass — the cluster may be on a VPN
        # that the Studio host can't reach directly. If EVERYTHING is unreachable it's
        # more likely a manifest error, but we still let the user proceed.
        ok = True
        self._step_end("verify", ok, message=msg)
        return ok

    async def _step_finalize(self) -> bool:
        self._step_start("finalize")
        urls = self.stack.output_urls(self.host)
        conns = self.stack.output_connections(self.host)
        outputs = {"urls": urls, "connections": conns}
        store.set_outputs(self.install_id, outputs)
        self._emit("result", payload=outputs)
        try:
            from .evidence import capture
            rec = store.get(self.install_id)
            if rec:
                out_dir = capture(rec)
                outputs["evidence_dir"] = str(out_dir)
                store.set_outputs(self.install_id, outputs)
                self._log("finalize", "stdout", f"evidence captured: {out_dir}")
        except Exception as e:
            self._log("finalize", "stderr", f"evidence capture skipped: {e}")
        self._step_end("finalize", True)
        return True

    async def run(self, env_overrides: dict[str, str], *, start_at: str = "verify") -> None:
        try:
            self._set_state("INSPECTING")
            self._set_state("READY_TO_INSTALL")

            start_idx = self._step_index(start_at)
            if start_idx < 0:
                return self._fail(f"unknown start step: {start_at}")

            for step_id, state in self._PIPELINE[start_idx:]:
                if self._cancel:
                    return self._fail("cancelled")
                if state != "READY":
                    self._set_state(state)
                if step_id == "verify":
                    ok = await self._step_verify()
                elif step_id == "finalize":
                    ok = await self._step_finalize()
                else:
                    ok = False
                if not ok:
                    return self._fail(f"{step_id} failed")

            self._set_state("READY")
            self._emit("state", status="READY")
            try:
                await notify(
                    self.install_id,
                    "install_completed",
                    "info",
                    f"Remote cluster connected: {self.stack.id}",
                    f"Cluster outputs captured. Access the stack via the links below.",
                    links={"success": f"/installs/{self.install_id}"},
                )
            except Exception:
                pass
        except asyncio.CancelledError:
            self._fail("cancelled")
        except Exception as e:
            import traceback as _tb
            self._fail(f"unexpected: {type(e).__name__}: {e} | {_tb.format_exc()}")

    async def cancel(self) -> None:
        self._cancel = True


class RemoteSSHRunner(UDPRunner):
    """Runs the full UDPRunner pipeline on a remote host via SSH + rsync.

    File preparation (clone + env patching) happens locally in a staging dir.
    Once the env step completes, the staged files are rsynced to the remote.
    All subsequent docker/compose commands run on the remote host via SSH.
    """

    def __init__(
        self,
        stack: StackManifest,
        install_id: str,
        host: str,
        local_staging_dir: Path,
        remote_dir: str,
        ssh_user: str,
        ssh_port: int = 22,
        ssh_key_path: Optional[str] = None,
        ssh_password: Optional[str] = None,
    ):
        super().__init__(stack, install_id, host, local_staging_dir)
        self.remote_dir = remote_dir
        self.ssh_user = ssh_user
        self.ssh_port = ssh_port
        self.ssh_key_path = ssh_key_path
        self.ssh_password = ssh_password

    # ── SSH helpers ──────────────────────────────────────────────────────────

    def _ssh_base_opts(self) -> list[str]:
        opts = [
            "-o", "StrictHostKeyChecking=no",
            "-o", "ConnectTimeout=30",
            "-o", "ServerAliveInterval=30",
            "-p", str(self.ssh_port),
        ]
        if self.ssh_key_path:
            opts += ["-i", self.ssh_key_path]
        return opts

    def _ssh_target(self) -> str:
        return f"{self.ssh_user}@{self.host}"

    def _sshpass_prefix(self) -> list[str]:
        """Return ['sshpass', '-p', password] prefix when password auth is used."""
        if not self.ssh_password:
            return []
        if shutil.which("sshpass"):
            return ["sshpass", "-p", self.ssh_password]
        raise RuntimeError(
            "sshpass is required for password authentication but is not installed. "
            "Run: sudo apt-get install -y sshpass"
        )

    async def _run_ssh(self, step_id: str, remote_cmd: str, timeout: int = 600) -> int:
        """Run a shell command on the remote host, streaming logs back."""
        opts = self._ssh_base_opts()
        full_cmd = f"cd {self._sh_quote(self.remote_dir)} && {remote_cmd}"
        argv = self._sshpass_prefix() + ["ssh"] + opts + [self._ssh_target(), full_cmd]
        return await self._run_bash(step_id, argv, Path("/tmp"), timeout)

    async def _sync_to_remote(self, step_id: str) -> bool:
        """rsync the local staging dir to the remote host."""
        opts = self._ssh_base_opts()
        # Build the SSH command string for rsync's -e option.
        # sshpass must wrap ssh inside the -e string when using password auth.
        if self.ssh_password and shutil.which("sshpass"):
            ssh_cmd = f"sshpass -p {self._sh_quote(self.ssh_password)} ssh " + " ".join(opts)
        else:
            ssh_cmd = "ssh " + " ".join(opts)
        src = str(self.install_dir).rstrip("/") + "/"
        dst = f"{self._ssh_target()}:{self.remote_dir}"
        self._log(step_id, "stdout", f"[remote] syncing files to {dst}...")
        rc = await self._run_bash(
            step_id,
            ["rsync", "-avz", "--delete", "-e", ssh_cmd, src, dst],
            Path("/tmp"),
            timeout=300,
        )
        return rc == 0

    # ── Overridden pipeline steps ─────────────────────────────────────────

    async def _step_prepare(self) -> bool:
        """Create local staging dir + remote install dir."""
        ok = await super()._step_prepare()
        if not ok:
            return False
        self._log("prepare", "stdout", f"[remote] creating {self.remote_dir} on {self.host}...")
        rc = await self._run_bash(
            "prepare",
            self._sshpass_prefix() + ["ssh"] + self._ssh_base_opts() + [
                self._ssh_target(),
                f"mkdir -p {self._sh_quote(self.remote_dir)}",
            ],
            Path("/tmp"),
            timeout=30,
        )
        if rc != 0:
            self._step_end("prepare", False, exit_code=rc,
                           message=f"Could not create {self.remote_dir} on remote host")
            return False
        return True

    async def _step_env(self, overrides: dict[str, str]) -> bool:
        """Patch files locally (super), then rsync the whole tree to remote."""
        ok = await super()._step_env(overrides)
        if not ok:
            return False
        # The super call wrote _step_end("env", True). We need to rsync now.
        # Re-open the step so additional log lines appear correctly.
        self._log("env", "stdout", "[remote] syncing patched files to remote host...")
        synced = await self._sync_to_remote("env")
        if not synced:
            self._step_end("env", False, exit_code=1,
                           message="rsync to remote host failed")
            return False
        # Fix permissions on the remote (chmod +x scripts)
        await self._run_ssh(
            "env",
            "chmod +x udp scripts/*.sh 2>/dev/null || true",
            timeout=30,
        )
        return True

    async def _step_cmd(self, step_id: str, cmd_name: str) -> bool:
        """Run a stack command (doctor / start / bootstrap / smoke) on remote."""
        self._step_start(step_id)
        try:
            spec = self.stack.command(cmd_name)
        except KeyError as e:
            self._step_end(step_id, False, message=str(e))
            return False

        argv: list[str] = list(spec["argv"])
        timeout: int = int(spec.get("timeout", 300))

        # Rewrite the command for remote execution. The argv from the manifest
        # is a local-path command list like ["bash", "scripts/lhs-bootstrap.sh"].
        # We join it for execution in the remote shell (cd already provided by
        # _run_ssh).
        import shlex as _shlex
        remote_cmd = " ".join(_shlex.quote(a) for a in argv)
        rc = await self._run_ssh(step_id, remote_cmd, timeout=timeout)
        ok = rc == 0
        self._step_end(step_id, ok, exit_code=rc)
        return ok

    def _fail(self, msg: str) -> None:
        store.update_state(self.install_id, "FAILED", error=msg)
        self._emit("state", status="FAILED")
        self._emit("error", line=msg)

    def _completion_body(self) -> str:
        return f"Remote install on {self.host} complete. Access the stack via the links in the install page."


def _make_runner(
    stack: StackManifest,
    install_id: str,
    host: str,
    install_dir: Path,
    ssh_user: Optional[str] = None,
    ssh_port: int = 22,
    ssh_key_path: Optional[str] = None,
    ssh_password: Optional[str] = None,
):
    if stack.is_remote_cluster:
        return RemoteClusterRunner(stack, install_id, host, install_dir)
    _is_local = host in ("localhost", "127.0.0.1", "::1")
    if not _is_local and ssh_user:
        from .config import WORK_DIR
        local_staging = WORK_DIR / "remote-staging" / install_id
        return RemoteSSHRunner(
            stack, install_id, host,
            local_staging_dir=local_staging,
            remote_dir=str(install_dir),
            ssh_user=ssh_user,
            ssh_port=ssh_port,
            ssh_key_path=ssh_key_path,
            ssh_password=ssh_password,
        )
    return UDPRunner(stack, install_id, host, install_dir)


def make_steps(stack: StackManifest) -> list[StepStatus]:
    return _build_steps(stack)


def next_step_id(stack: StackManifest, step_id: str) -> str | None:
    if stack.is_remote_cluster:
        pipeline = [sid for sid, _ in RemoteClusterRunner._PIPELINE]
    else:
        pipeline = [sid for sid, _ in UDPRunner._PIPELINE]
    try:
        i = pipeline.index(step_id)
    except ValueError:
        return None
    return pipeline[i + 1] if i + 1 < len(pipeline) else None


async def retry_install(stack: StackManifest, install_id: str, host: str, install_dir: Path,
                        env_overrides: dict[str, str], start_at: str) -> None:
    """Resume a failed install from `start_at`. Resets the chosen step (and
    everything after) to pending before re-running so the UI updates cleanly.
    """
    rec = store.get(install_id)
    if not rec:
        return
    if stack.is_remote_cluster:
        pipeline = [sid for sid, _ in RemoteClusterRunner._PIPELINE]
    else:
        pipeline = [sid for sid, _ in UDPRunner._PIPELINE]
    if start_at not in pipeline:
        return
    cutover = pipeline.index(start_at)
    for s in rec.steps:
        if s.id in pipeline and pipeline.index(s.id) >= cutover:
            s.status = "pending"
            s.started_at = None
            s.finished_at = None
            s.exit_code = None
            s.message = None
    rec.error = None
    store._persist()
    runner = _make_runner(
        stack, install_id, host, install_dir,
        ssh_user=rec.ssh_user, ssh_port=rec.ssh_port, ssh_key_path=rec.ssh_key_path,
    )
    await runner.run(env_overrides, start_at=start_at)


def mark_step_skipped(install_id: str, step_id: str) -> str | None:
    """Mark a step as skipped (only allowed for SKIPPABLE steps). Return the next step id, or None."""
    all_skippable = UDPRunner.SKIPPABLE | RemoteSSHRunner.SKIPPABLE | RemoteClusterRunner.SKIPPABLE
    if step_id not in all_skippable:
        return None
    rec = store.get(install_id)
    if not rec:
        return None
    for s in rec.steps:
        if s.id == step_id:
            s.status = "skipped"
            s.message = "user-skipped"
            break
    store._persist()
    return next_step_id_for(step_id)


def next_step_id_for(step_id: str) -> str | None:
    for pipeline_def in [UDPRunner._PIPELINE, RemoteClusterRunner._PIPELINE]:
        pipeline = [sid for sid, _ in pipeline_def]
        if step_id in pipeline:
            i = pipeline.index(step_id)
            return pipeline[i + 1] if i + 1 < len(pipeline) else None
    return None


async def run_command(install_id: str, install_dir: Path, host: str, stack: StackManifest, cmd_name: str) -> int:
    """One-shot command for stop/clean/status, with logs piped through the event bus."""
    runner = UDPRunner(stack, install_id, host, install_dir)
    runner._step_start(cmd_name)
    try:
        spec = stack.command(cmd_name)
    except KeyError as e:
        runner._step_end(cmd_name, False, message=str(e))
        return 1
    rc = await runner._run_bash(cmd_name, list(spec["argv"]), install_dir, int(spec.get("timeout", 300)))
    runner._step_end(cmd_name, rc == 0, exit_code=rc)
    return rc
