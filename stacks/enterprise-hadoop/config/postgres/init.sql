-- ╔══════════════════════════════════════════════════════════════════════════╗
-- ║  Enterprise On-Prem Datalake — PostgreSQL bootstrap                      ║
-- ║                                                                          ║
-- ║  Live cluster runs TWO Postgres 14 servers (stg01 + afw01). In this      ║
-- ║  single-host docker replica we collapse them to ONE instance with three  ║
-- ║  databases (metastore + ranger + airflow). Roles and passwords match     ║
-- ║  the live cluster's configured users.                                    ║
-- ╚══════════════════════════════════════════════════════════════════════════╝

-- ── Databases ─────────────────────────────────────────────────────────────
CREATE DATABASE metastore;
CREATE DATABASE airflow;
CREATE DATABASE ranger;

-- ── Hive Metastore role (live: hive / HiveAdmin) ──────────────────────────
CREATE USER hive WITH PASSWORD 'HiveAdmin';
GRANT ALL PRIVILEGES ON DATABASE metastore TO hive;
ALTER DATABASE metastore OWNER TO hive;

-- ── Ranger role ───────────────────────────────────────────────────────────
CREATE USER ranger WITH PASSWORD 'ranger';
GRANT ALL PRIVILEGES ON DATABASE ranger TO ranger;
ALTER DATABASE ranger OWNER TO ranger;
GRANT ALL PRIVILEGES ON DATABASE ranger TO postgres;

-- ── Airflow role (Celery executor needs full read/write) ──────────────────
-- Default airflow image connects as 'postgres' (root) — no explicit airflow user.
-- Leave the database owned by postgres so the entrypoint can run migrations.
GRANT ALL PRIVILEGES ON DATABASE airflow TO postgres;
