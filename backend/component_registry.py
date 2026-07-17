"""Component registry — every buildable lakehouse component.

Each entry defines:
  - Docker image + default version
  - Exposed ports
  - Required dependencies (auto-added when this component is selected)
  - Config role (informs ai_configurator wiring)
  - Health check command
  - Service-level docker-compose YAML template

The registry is the single source of truth for dynamic stack composition.
stack_composer.py reads it to build docker-compose.yml for any selection.
"""
from __future__ import annotations

from typing import Any

# ---------------------------------------------------------------------------
# Component definitions
# ---------------------------------------------------------------------------

COMPONENTS: dict[str, dict[str, Any]] = {

    # ── Object Storage ──────────────────────────────────────────────────────

    "minio": {
        "name": "MinIO",
        "category": "object_storage",
        "image": "minio/minio",
        "default_version": "RELEASE.2025-04-22T22-12-26Z",
        "ports": [9000, 9001],
        "depends_on": [],
        "config_roles": ["s3_store"],
        "env": {
            "MINIO_ROOT_USER": "${MINIO_ROOT_USER:-admin}",
            "MINIO_ROOT_PASSWORD": "${MINIO_ROOT_PASSWORD:-udp_admin_12345}",
        },
        "command": 'server /data --console-address ":9001"',
        "volumes": ["minio_data:/data"],
        "healthcheck": {
            "test": ["CMD-SHELL", "curl -fsS http://localhost:9000/minio/health/live >/dev/null"],
            "interval": "15s", "timeout": "5s", "retries": 20, "start_period": "10s",
        },
        "description": "S3-compatible local object store — the default warehouse.",
    },

    "minio-create-bucket": {
        "name": "MinIO Bucket Init",
        "category": "init",
        "image": "minio/mc",
        "default_version": "latest",
        "restart_policy": "no",   # one-shot job — exits 0, must not loop
        "ports": [],
        "depends_on": ["minio"],
        "config_roles": ["bucket_init"],
        "env": {},
        "entrypoint": "/bin/sh",
        "command": (
            '-c "until mc alias set local http://minio:9000 '
            '$${MINIO_ROOT_USER:-admin} $${MINIO_ROOT_PASSWORD:-udp_admin_12345} '
            '>/dev/null 2>&1; do sleep 2; done; '
            'mc mb --ignore-existing local/datalake; '
            'mc mb --ignore-existing local/warehouse; echo bucket-init-done"'
        ),
        "description": "Creates the datalake and warehouse buckets on first boot.",
    },

    "hdfs": {
        "name": "HDFS (NameNode + DataNode)",
        "category": "object_storage",
        "image": "apache/hadoop",
        "default_version": "3.4.1",
        "ports": [9870, 9864, 8020],
        "depends_on": [],
        "config_roles": ["hdfs_store"],
        "user": "root",
        "env": {
            "HADOOP_HOME": "/opt/hadoop",
            "CLUSTER_NAME": "lakehouse",
            "HDFS_DATANODE_USER": "root",
            "HDFS_NAMENODE_USER": "root",
            "HDFS_SECONDARYNAMENODE_USER": "root",
        },
        "command": [
            "/bin/bash", "-c",
            (
                "mkdir -p /hadoop/dfs/name /hadoop/dfs/data && "
                "chmod 700 /hadoop/dfs/name /hadoop/dfs/data && "
                "test -f /hadoop/dfs/name/current/VERSION || "
                "hdfs namenode -format -force -nonInteractive; "
                "hdfs --daemon start namenode; "
                "sleep 5; "
                "exec hdfs datanode"
            ),
        ],
        "volumes": [
            "hdfs_namenode:/hadoop/dfs/name",
            "hdfs_datanode:/hadoop/dfs/data",
            "./config/hadoop/core-site.xml:/opt/hadoop/etc/hadoop/core-site.xml:ro",
            "./config/hadoop/hdfs-site.xml:/opt/hadoop/etc/hadoop/hdfs-site.xml:ro",
        ],
        "healthcheck": {
            "test": ["CMD-SHELL", "curl -fsS http://localhost:9870/jmx >/dev/null"],
            "interval": "20s", "timeout": "5s", "retries": 15, "start_period": "30s",
        },
        "description": "Hadoop Distributed File System — alternative to MinIO for on-prem.",
    },

    # ── Catalog / Metastore ─────────────────────────────────────────────────

    "mysql-hms": {
        "name": "MySQL 8 (HMS backing DB)",
        "category": "metastore_db",
        "image": "mysql",
        "default_version": "8.0",
        "ports": [],          # expose only internally
        "depends_on": [],
        "config_roles": ["hms_db"],
        "env": {
            "MYSQL_DATABASE": "metastore",
            "MYSQL_USER": "hive",
            "MYSQL_PASSWORD": "${HMS_DB_PASSWORD:-hive_password_pilot}",
            "MYSQL_ROOT_PASSWORD": "${HMS_DB_ROOT_PASSWORD:-root_password_pilot}",
        },
        "volumes": ["mysql_hms_data:/var/lib/mysql"],
        "healthcheck": {
            "test": ["CMD-SHELL", 'mysql -h 127.0.0.1 -uhive -p$${MYSQL_PASSWORD} -D metastore -e "SELECT 1" >/dev/null'],
            "interval": "10s", "timeout": "5s", "retries": 30, "start_period": "60s",
        },
        "description": "MySQL 8 backing database for Hive Metastore.",
    },

    "postgres": {
        "name": "PostgreSQL 15",
        "category": "metastore_db",
        "image": "postgres",
        "default_version": "15-alpine",
        "ports": ["5533:5432"],
        "depends_on": [],
        "config_roles": ["postgres_db"],
        "env": {
            "POSTGRES_USER": "${POSTGRES_USER:-lakehouse}",
            "POSTGRES_PASSWORD": "${POSTGRES_PASSWORD:-lakehouse_pass}",
            "POSTGRES_DB": "${POSTGRES_DB:-lakehouse}",
        },
        "volumes": ["postgres_data:/var/lib/postgresql/data"],
        "healthcheck": {
            "test": ["CMD-SHELL", "pg_isready -U $${POSTGRES_USER:-lakehouse}"],
            "interval": "10s", "timeout": "5s", "retries": 20, "start_period": "10s",
        },
        "description": "PostgreSQL — backing DB for Airflow, Dagster, Superset, Ranger.",
    },

    "hive-metastore": {
        "name": "Hive Metastore (Thrift)",
        "category": "catalog",
        "image": "bitsondatadev/hive-metastore",
        "default_version": "latest",
        "ports": [9083],
        "depends_on": ["mysql-hms"],
        "config_roles": ["hive_catalog"],
        "env": {
            "METASTORE_DB_HOSTNAME": "mysql-hms",
        },
        "volumes": ["./hive-metastore-site.xml:/opt/apache-hive-metastore-3.0.0-bin/conf/metastore-site.xml:ro"],
        "healthcheck": {
            "test": ["CMD-SHELL", "bash -c '</dev/tcp/127.0.0.1/9083'"],
            "interval": "15s", "timeout": "5s", "retries": 30, "start_period": "90s",
        },
        "description": "Hive Metastore Thrift server — schema registry for Hudi/Delta/Iceberg tables.",
    },

    "nessie": {
        "name": "Project Nessie",
        "category": "catalog",
        "image": "ghcr.io/projectnessie/nessie",
        "default_version": "0.99.0",
        "ports": [19120],
        "depends_on": [],
        "config_roles": ["nessie_catalog"],
        "env": {
            "NESSIE_VERSION_STORE_TYPE": "IN_MEMORY",
            "QUARKUS_OIDC_TENANT_ENABLED": "false",
        },
        "volumes": ["./nessie.properties:/deployments/config/application.properties:ro"],
        "healthcheck": {
            # /q/health is on the Quarkus mgmt port 9000; the API on 19120 is what
            # clients use — check that instead.
            "test": ["CMD", "curl", "-fsS", "http://localhost:19120/api/v1/config"],
            "interval": "10s", "timeout": "5s", "retries": 12, "start_period": "20s",
        },
        "description": "Git-like version control for your data lake (Iceberg + REST).",
    },

    "iceberg-rest": {
        "name": "Iceberg REST Catalog",
        "category": "catalog",
        "image": "tabulario/iceberg-rest",
        "default_version": "1.6.0",
        "ports": [8181],
        "depends_on": ["minio"],
        "config_roles": ["iceberg_rest_catalog"],
        "env": {
            "CATALOG_WAREHOUSE": "s3://warehouse",
            "CATALOG_IO__IMPL": "org.apache.iceberg.aws.s3.S3FileIO",
            "CATALOG_S3_ENDPOINT": "http://udp-minio:9000",
            # path-style access — otherwise S3FileIO uses virtual-host style
            # (warehouse.udp-minio:9000) which does not resolve in Docker DNS.
            "CATALOG_S3_PATH__STYLE__ACCESS": "true",
            "AWS_ACCESS_KEY_ID": "${MINIO_ROOT_USER:-admin}",
            "AWS_SECRET_ACCESS_KEY": "${MINIO_ROOT_PASSWORD:-udp_admin_12345}",
            "AWS_REGION": "us-east-1",
        },
        "healthcheck": {
            # tabulario/iceberg-rest has no curl — use bash /dev/tcp port check
            "test": ["CMD-SHELL", "bash -c '</dev/tcp/localhost/8181'"],
            "interval": "10s", "timeout": "5s", "retries": 12, "start_period": "20s",
        },
        "description": "Iceberg REST catalog — lightweight, no external DB required.",
    },

    # ── Compute ─────────────────────────────────────────────────────────────

    "spark": {
        "name": "Apache Spark",
        "category": "compute",
        "image": "apache/spark",
        "default_version": "3.5.5",
        "ports": [7077, 8888, 4040, 18080],
        "depends_on": ["minio-create-bucket"],
        "config_roles": ["spark_engine"],
        "env": {
            "SPARK_MODE": "master",
            "SPARK_MASTER_HOST": "spark",
            "AWS_ACCESS_KEY_ID": "${MINIO_ROOT_USER:-admin}",
            "AWS_SECRET_ACCESS_KEY": "${MINIO_ROOT_PASSWORD:-udp_admin_12345}",
        },
        "volumes": [
            "./config/spark/spark-defaults.conf:/opt/spark/conf/spark-defaults.conf:ro",
            "./config/hadoop/core-site.xml:/opt/spark/conf/core-site.xml:ro",
            "./config/hive/hive-site.xml:/opt/spark/conf/hive-site.xml:ro",
        ],
        "healthcheck": {
            "test": ["CMD-SHELL", "curl -fsS http://localhost:8888 >/dev/null"],
            "interval": "15s", "timeout": "5s", "retries": 20, "start_period": "30s",
        },
        "description": "Apache Spark master node — distributed compute for lakehouse tables.",
    },

    "spark-hudi": {
        "name": "Spark + Hudi",
        "category": "compute",
        "image": "lakehousestudio/spark-hudi",
        "default_version": "3.5.5_1.2.0",
        "build_dockerfile": "scripts/images/Dockerfile.spark-hudi",
        "ports": [7077, 8888, 4040, 18080],
        "depends_on": ["minio-create-bucket"],
        "config_roles": ["spark_engine", "hudi_format"],
        "env": {
            "SPARK_MODE": "master",
            "SPARK_MASTER_HOST": "spark",
            "AWS_ACCESS_KEY_ID": "${MINIO_ROOT_USER:-admin}",
            "AWS_SECRET_ACCESS_KEY": "${MINIO_ROOT_PASSWORD:-udp_admin_12345}",
        },
        "volumes": [
            "./config/spark/spark-defaults.conf:/opt/spark/conf/spark-defaults.conf:ro",
            "./config/hadoop/core-site.xml:/opt/spark/conf/core-site.xml:ro",
            "./config/hive/hive-site.xml:/opt/spark/conf/hive-site.xml:ro",
        ],
        "healthcheck": {
            "test": ["CMD-SHELL", "/opt/spark/bin/spark-submit --version >/dev/null 2>&1"],
            "interval": "15s", "timeout": "10s", "retries": 20, "start_period": "30s",
        },
        "description": "Spark with Hudi bundle pre-baked — write Hudi CoW/MoR tables.",
    },

    "spark-delta": {
        "name": "Spark + Delta Lake",
        "category": "compute",
        "image": "lakehousestudio/spark-delta",
        "default_version": "3.5.5_3.3.2",
        "build_dockerfile": "scripts/images/Dockerfile.spark-delta",
        "ports": [7077, 8888, 4040, 18080],
        "depends_on": ["minio-create-bucket"],
        "config_roles": ["spark_engine", "delta_format"],
        "env": {
            "SPARK_MODE": "master",
            "SPARK_MASTER_HOST": "spark",
            "AWS_ACCESS_KEY_ID": "${MINIO_ROOT_USER:-admin}",
            "AWS_SECRET_ACCESS_KEY": "${MINIO_ROOT_PASSWORD:-udp_admin_12345}",
        },
        "volumes": [
            "./config/spark/spark-defaults.conf:/opt/spark/conf/spark-defaults.conf:ro",
            "./config/hadoop/core-site.xml:/opt/spark/conf/core-site.xml:ro",
            "./config/hive/hive-site.xml:/opt/spark/conf/hive-site.xml:ro",
        ],
        "healthcheck": {
            "test": ["CMD-SHELL", "/opt/spark/bin/spark-submit --version >/dev/null 2>&1"],
            "interval": "15s", "timeout": "10s", "retries": 20, "start_period": "30s",
        },
        "description": "Spark with Delta Lake JARs pre-baked.",
    },

    "spark-iceberg": {
        "name": "Spark + Iceberg",
        "category": "compute",
        "image": "tabulario/spark-iceberg",
        "default_version": "3.5.5_1.8.1",
        "ports": [7077, 8888, 4040, 18080],
        "depends_on": ["minio-create-bucket"],
        "config_roles": ["spark_engine", "iceberg_format"],
        "env": {
            "SPARK_MODE": "master",
            "SPARK_MASTER_HOST": "spark",
            "AWS_ACCESS_KEY_ID": "${MINIO_ROOT_USER:-admin}",
            "AWS_SECRET_ACCESS_KEY": "${MINIO_ROOT_PASSWORD:-udp_admin_12345}",
        },
        "volumes": [
            "./config/spark/spark-defaults.conf:/opt/spark/conf/spark-defaults.conf:ro",
            "./config/hadoop/core-site.xml:/opt/spark/conf/core-site.xml:ro",
            "./config/hive/hive-site.xml:/opt/spark/conf/hive-site.xml:ro",
        ],
        "healthcheck": {
            "test": ["CMD-SHELL", "/opt/spark/bin/spark-submit --version >/dev/null 2>&1"],
            "interval": "15s", "timeout": "10s", "retries": 20, "start_period": "30s",
        },
        "description": "Spark with Iceberg runtime pre-baked (tabulario image).",
    },

    # ── Query Engines ────────────────────────────────────────────────────────

    "trino": {
        "name": "Trino",
        "category": "query_engine",
        "image": "trinodb/trino",
        "default_version": "481",
        "ports": ["8285:8080"],
        "depends_on": [],
        "config_roles": ["sql_engine"],
        "env": {
            # Right-sized for memory-constrained dev hosts (8GB). Override via
            # TRINO_JAVA_OPTS for bigger machines. 3G was too large when Spark +
            # Nessie + Airflow JVMs share an 8GB box → Trino thrashed/restarted.
            "JAVA_TOOL_OPTIONS": "${TRINO_JAVA_OPTS:--Xms1G -Xmx1500m -XX:+UseG1GC}",
            "TRINO_QUERY_MAX_MEMORY_PER_NODE": "${TRINO_QUERY_MAX_MEMORY_PER_NODE:-800MB}",
        },
        "volumes": ["./config/trino/catalog:/etc/trino/catalog:ro"],
        "healthcheck": {
            "test": ["CMD-SHELL", "curl -fsS http://localhost:8080/v1/info >/dev/null"],
            "interval": "15s", "timeout": "5s", "retries": 40, "start_period": "90s",
        },
        "description": "Federated SQL engine — query Iceberg/Hudi/Delta + external sources.",
    },

    "starrocks": {
        "name": "StarRocks",
        "category": "query_engine",
        "image": "starrocks/allin1-ubuntu",
        "default_version": "3.3.9",
        "ports": [8030, 9030, 8040, 9040],
        "depends_on": [],
        "config_roles": ["olap_engine"],
        "env": {},
        "healthcheck": {
            "test": ["CMD-SHELL", "mysql -h 127.0.0.1 -P 9030 -u root -e 'SELECT 1' >/dev/null 2>&1"],
            "interval": "20s", "timeout": "5s", "retries": 30, "start_period": "60s",
        },
        "description": "MPP OLAP engine with native Iceberg/Hudi/Delta external catalog.",
    },

    # ── Streaming ─────────────────────────────────────────────────────────────

    "kafka": {
        "name": "Apache Kafka",
        "category": "streaming",
        "image": "confluentinc/cp-kafka",
        "default_version": "7.8.0",
        "ports": [9092, 9101],
        "depends_on": ["zookeeper"],
        "config_roles": ["message_bus"],
        "env": {
            "KAFKA_BROKER_ID": "1",
            "KAFKA_ZOOKEEPER_CONNECT": "zookeeper:2181",
            "KAFKA_LISTENERS": "PLAINTEXT://0.0.0.0:29092,PLAINTEXT_HOST://0.0.0.0:9092",
            "KAFKA_ADVERTISED_LISTENERS": "PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092",
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT",
            "KAFKA_INTER_BROKER_LISTENER_NAME": "PLAINTEXT",
            "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1",
        },
        "depends_on": ["zookeeper"],
        "healthcheck": {
            # confluentinc/cp-kafka ships `kafka-topics` (no .sh suffix).
            "test": ["CMD-SHELL", "kafka-topics --bootstrap-server localhost:9092 --list >/dev/null 2>&1"],
            "interval": "20s", "timeout": "10s", "retries": 15, "start_period": "30s",
        },
        "description": "Distributed event streaming platform.",
    },

    "zookeeper": {
        "name": "ZooKeeper (Kafka dependency)",
        "category": "infra",
        "image": "confluentinc/cp-zookeeper",
        "default_version": "7.8.0",
        "ports": [2181],
        "depends_on": [],
        "config_roles": ["zk_coord"],
        "env": {
            "ZOOKEEPER_CLIENT_PORT": "2181",
            "ZOOKEEPER_TICK_TIME": "2000",
        },
        "volumes": ["zookeeper_data:/var/lib/zookeeper/data"],
        "description": "ZooKeeper coordination service (required by Kafka).",
    },

    "flink": {
        "name": "Apache Flink",
        "category": "streaming",
        "image": "flink",
        "default_version": "1.20-scala_2.12",
        "ports": ["8180:8081", 6123],
        "depends_on": ["kafka"],
        "config_roles": ["stream_processor"],
        # Official flink image needs an explicit role command; without it the
        # container just prints usage and exits. Run the JobManager (REST :8081).
        "command": "jobmanager",
        # Configure via FLINK_PROPERTIES (the image's documented mechanism — the
        # entrypoint appends these to conf/config.yaml). Do NOT mount a legacy
        # flink-conf.yaml: Flink 1.20 reads config.yaml, and a partial mounted
        # flink-conf.yaml shadows it and drops the required memory keys, crashing
        # the JobManager with "jobmanager.memory.process.size not configured".
        "env": {
            "FLINK_PROPERTIES": (
                "jobmanager.rpc.address: udp-flink\n"
                "jobmanager.memory.process.size: 1024m\n"
                "taskmanager.memory.process.size: 1280m\n"
                "taskmanager.numberOfTaskSlots: 2\n"
                "rest.address: 0.0.0.0\n"
                "rest.bind-address: 0.0.0.0"
            ),
        },
        "healthcheck": {
            "test": ["CMD-SHELL", "curl -fsS http://localhost:8081/overview >/dev/null"],
            "interval": "20s", "timeout": "5s", "retries": 15, "start_period": "30s",
        },
        "description": "Stateful stream processing — CDC pipelines to Iceberg/Hudi.",
    },

    "kafka-connect": {
        "name": "Kafka Connect",
        "category": "streaming",
        "image": "confluentinc/cp-kafka-connect",
        "default_version": "7.8.0",
        "ports": [8083],
        "depends_on": ["kafka"],
        "config_roles": ["kafka_connect"],
        "env": {
            "CONNECT_BOOTSTRAP_SERVERS": "kafka:29092",
            "CONNECT_REST_PORT": "8083",
            "CONNECT_GROUP_ID": "lakehouse-connect",
            "CONNECT_CONFIG_STORAGE_TOPIC": "_connect-configs",
            "CONNECT_OFFSET_STORAGE_TOPIC": "_connect-offsets",
            "CONNECT_STATUS_STORAGE_TOPIC": "_connect-status",
            "CONNECT_KEY_CONVERTER": "org.apache.kafka.connect.json.JsonConverter",
            "CONNECT_VALUE_CONVERTER": "org.apache.kafka.connect.json.JsonConverter",
        },
        "description": "Kafka Connect for CDC and data ingestion pipelines.",
    },

    # ── Orchestration ─────────────────────────────────────────────────────────

    "airflow": {
        "name": "Apache Airflow",
        "category": "orchestration",
        "image": "apache/airflow",
        "default_version": "3.2.1",
        "ports": ["8090:8080"],
        "depends_on": ["postgres"],
        "config_roles": ["dag_scheduler"],
        "command": "standalone",
        "env": {
            "AIRFLOW__CORE__EXECUTOR": "LocalExecutor",
            # Use hostname-based URL so it resolves inside Docker network regardless of project name
            "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": "postgresql+psycopg2://${POSTGRES_USER:-lakehouse}:${POSTGRES_PASSWORD:-lakehouse_pass}@udp-postgres/${POSTGRES_DB:-lakehouse}",
            "AIRFLOW__CORE__FERNET_KEY": "${AIRFLOW_FERNET_KEY:-ZmDfcTF7_60GrrY167zsiPd67pEvs0aGOv2oasOM1Pg=}",
            "AIRFLOW__WEBSERVER__SECRET_KEY": "${AIRFLOW_SECRET_KEY:-lakehouse_secret}",
            "AIRFLOW_UID": "50000",
            "_AIRFLOW_DB_MIGRATE": "true",
            # Airflow 3.x simple-auth-manager env vars
            "_AIRFLOW_WWW_USER_CREATE": "true",
            "_AIRFLOW_WWW_USER_USERNAME": "admin",
            "_AIRFLOW_WWW_USER_PASSWORD": "admin",
            "AIRFLOW__SIMPLE_AUTH_MANAGER__PASSWORDS": "admin:admin",
        },
        "volumes": ["./airflow/dags:/opt/airflow/dags", "./airflow/logs:/opt/airflow/logs"],
        "healthcheck": {
            "test": ["CMD-SHELL", "airflow jobs check --hostname $$(hostname) >/dev/null 2>&1"],
            "interval": "30s", "timeout": "10s", "retries": 10, "start_period": "60s",
        },
        "description": "DAG-based workflow scheduler — schedule Spark/Trino/dbt pipelines.",
    },

    "dagster": {
        "name": "Dagster",
        "category": "orchestration",
        "image": "dagster/dagster-celery-docker",
        "default_version": "1.9.4",
        "ports": ["3001:3000"],
        "depends_on": ["postgres"],
        "config_roles": ["asset_scheduler"],
        "env": {
            "DAGSTER_POSTGRES_HOST": "postgres",
            "DAGSTER_POSTGRES_PORT": "5432",
            "DAGSTER_POSTGRES_USER": "${POSTGRES_USER:-lakehouse}",
            "DAGSTER_POSTGRES_PASSWORD": "${POSTGRES_PASSWORD:-lakehouse_pass}",
            "DAGSTER_POSTGRES_DB": "${POSTGRES_DB:-lakehouse}",
        },
        "volumes": ["./dagster/workspace.yaml:/opt/dagster/dagster_home/workspace.yaml"],
        "healthcheck": {
            "test": ["CMD-SHELL", "curl -fsS http://localhost:3000/dagit/notebook >/dev/null 2>&1 || true"],
            "interval": "30s", "timeout": "10s", "retries": 10, "start_period": "60s",
        },
        "description": "Asset-first orchestrator — define pipelines by what data they produce.",
    },

    # ── Transformation ────────────────────────────────────────────────────────

    "dbt": {
        "name": "dbt Core",
        "category": "transformation",
        "image": "ghcr.io/dbt-labs/dbt-trino",
        "default_version": "1.9.0",
        "ports": [8585],
        "depends_on": ["trino"],
        "config_roles": ["sql_transform"],
        "env": {
            "DBT_PROFILES_DIR": "/usr/app/profiles",
            "DBT_PROJECT_DIR": "/usr/app/dbt",
        },
        "volumes": ["./dbt:/usr/app/dbt", "./dbt/profiles.yml:/usr/app/profiles/profiles.yml"],
        "description": "SQL-based transformation layer — dbt models run against Trino.",
    },

    # ── Security ──────────────────────────────────────────────────────────────

    "solr": {
        "name": "Apache Solr (Ranger audit)",
        "category": "infra",
        "image": "solr",
        "default_version": "8.11",
        "ports": [8983],
        "depends_on": [],
        "config_roles": ["audit_store"],
        "env": {},
        "volumes": ["solr_data:/var/solr"],
        "healthcheck": {
            "test": ["CMD-SHELL", "curl -fsS http://localhost:8983/solr/admin/info/system >/dev/null"],
            "interval": "15s", "timeout": "5s", "retries": 15, "start_period": "30s",
        },
        "description": "Solr — audit log store required by Ranger.",
    },

    # ── Connection Pooling ────────────────────────────────────────────────────

    "pgbouncer": {
        "name": "PgBouncer",
        "category": "connection_pooling",
        "image": "edoburu/pgbouncer",
        "default_version": "latest",
        # pgbouncer listens on 5432 inside the container; host port 5433 avoids conflict
        "ports": ["5433:5432"],
        "depends_on": ["postgres"],
        "config_roles": ["pg_pool"],
        "env": {
            "DB_HOST": "postgres",
            "DB_PORT": "5432",
            "DB_USER": "${POSTGRES_USER:-lakehouse}",
            "DB_PASSWORD": "${POSTGRES_PASSWORD:-lakehouse_pass}",
            "DB_NAME": "${POSTGRES_DB:-lakehouse}",
            "POOL_MODE": "transaction",
            "MAX_CLIENT_CONN": "200",
            "DEFAULT_POOL_SIZE": "20",
        },
        # pg_isready is available in the Alpine-based edoburu/pgbouncer image
        "healthcheck": {
            "test": ["CMD-SHELL", "pg_isready -h localhost -p 5432"],
            "interval": "15s", "timeout": "5s", "retries": 10,
        },
        "description": "Connection pooler for PostgreSQL — reduces connection overhead.",
    },

    # ── Monitoring ────────────────────────────────────────────────────────────

    "prometheus": {
        "name": "Prometheus",
        "category": "monitoring",
        "image": "prom/prometheus",
        "default_version": "v2.55.1",
        "ports": [9090],
        "depends_on": [],
        "config_roles": ["metrics_store"],
        "env": {},
        "volumes": ["./config/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro",
                    "prometheus_data:/prometheus"],
        "command": "--config.file=/etc/prometheus/prometheus.yml --storage.tsdb.path=/prometheus",
        "healthcheck": {
            "test": ["CMD-SHELL", "curl -fsS http://localhost:9090/-/healthy >/dev/null"],
            "interval": "15s", "timeout": "5s", "retries": 10, "start_period": "20s",
        },
        "description": "Pull-based metrics scraping for all lakehouse services.",
    },

    "grafana": {
        "name": "Grafana",
        "category": "monitoring",
        "image": "grafana/grafana",
        "default_version": "11.3.1",
        "ports": ["3010:3000"],
        "depends_on": ["prometheus"],
        "config_roles": ["dashboard"],
        "env": {
            "GF_SECURITY_ADMIN_USER": "${GRAFANA_USER:-admin}",
            "GF_SECURITY_ADMIN_PASSWORD": "${GRAFANA_PASSWORD:-admin}",
            "GF_USERS_ALLOW_SIGN_UP": "false",
        },
        "volumes": ["grafana_data:/var/lib/grafana",
                    "./config/grafana/datasources:/etc/grafana/provisioning/datasources"],
        "healthcheck": {
            "test": ["CMD-SHELL", "curl -fsS http://localhost:3000/api/health >/dev/null"],
            "interval": "15s", "timeout": "5s", "retries": 10, "start_period": "20s",
        },
        "description": "Metrics dashboards pre-wired to Prometheus + MinIO + Trino + StarRocks.",
    },

    "loki": {
        "name": "Grafana Loki",
        "category": "monitoring",
        "image": "grafana/loki",
        "default_version": "3.3.2",
        "ports": [3100],
        "depends_on": [],
        "config_roles": ["log_store"],
        "env": {},
        "volumes": ["./config/loki/loki.yml:/etc/loki/loki.yml:ro", "loki_data:/loki"],
        "command": "-config.file=/etc/loki/loki.yml",
        "healthcheck": {
            "test": ["CMD-SHELL", "curl -fsS http://localhost:3100/ready >/dev/null"],
            "interval": "15s", "timeout": "5s", "retries": 10,
        },
        "description": "Log aggregation — ships lakehouse service logs to Grafana.",
    },

    # ── BI Layer ──────────────────────────────────────────────────────────────

    "superset": {
        "name": "Apache Superset",
        "category": "bi",
        "image": "apache/superset",
        "default_version": "4.1.1",
        "ports": ["8089:8088"],
        "depends_on": ["postgres"],
        "config_roles": ["bi_tool"],
        "env": {
            "SUPERSET_SECRET_KEY": "${SUPERSET_SECRET_KEY:-superset_secret_lakehouse}",
            "SQLALCHEMY_DATABASE_URI": "postgresql+psycopg2://${POSTGRES_USER:-lakehouse}:${POSTGRES_PASSWORD:-lakehouse_pass}@postgres/${POSTGRES_DB:-lakehouse}",
        },
        "healthcheck": {
            "test": ["CMD-SHELL", "curl -fsS http://localhost:8088/health >/dev/null"],
            "interval": "20s", "timeout": "5s", "retries": 15, "start_period": "60s",
        },
        "description": "Self-service BI pre-wired to Trino/StarRocks — 40+ chart types.",
    },

    # ── Hadoop ecosystem ──────────────────────────────────────────────────────

    "hadoop-yarn": {
        "name": "Apache YARN (ResourceManager)",
        "category": "hadoop",
        "image": "apache/hadoop",
        "default_version": "3.4.1",
        "ports": ["8188:8088"],
        "depends_on": ["hdfs"],
        "config_roles": ["yarn_rm"],
        "env": {
            "HADOOP_HOME": "/opt/hadoop",
            "CLUSTER_NAME": "lakehouse",
        },
        "command": "yarn resourcemanager",
        "volumes": [
            "./config/hadoop/core-site.xml:/opt/hadoop/etc/hadoop/core-site.xml:ro",
            "./config/hadoop/hdfs-site.xml:/opt/hadoop/etc/hadoop/hdfs-site.xml:ro",
            "./config/hadoop/yarn-site.xml:/opt/hadoop/etc/hadoop/yarn-site.xml:ro",
            "./config/hadoop/mapred-site.xml:/opt/hadoop/etc/hadoop/mapred-site.xml:ro",
        ],
        "healthcheck": {
            "test": ["CMD-SHELL", "curl -fsS http://localhost:8088/ws/v1/cluster/info >/dev/null"],
            "interval": "20s", "timeout": "5s", "retries": 15, "start_period": "30s",
        },
        "description": "YARN ResourceManager — cluster resource scheduler for Spark, Tez, and MR jobs.",
    },

    "hadoop-yarn-nm": {
        "name": "YARN NodeManager",
        "category": "hadoop",
        "image": "apache/hadoop",
        "default_version": "3.4.1",
        "ports": [8042],
        "depends_on": ["hadoop-yarn"],
        "config_roles": ["yarn_nm"],
        "env": {
            "HADOOP_HOME": "/opt/hadoop",
            "CLUSTER_NAME": "lakehouse",
        },
        "command": "yarn nodemanager",
        "volumes": [
            "./config/hadoop/core-site.xml:/opt/hadoop/etc/hadoop/core-site.xml:ro",
            "./config/hadoop/hdfs-site.xml:/opt/hadoop/etc/hadoop/hdfs-site.xml:ro",
            "./config/hadoop/yarn-site.xml:/opt/hadoop/etc/hadoop/yarn-site.xml:ro",
            "./config/hadoop/mapred-site.xml:/opt/hadoop/etc/hadoop/mapred-site.xml:ro",
        ],
        "healthcheck": {
            "test": ["CMD-SHELL", "curl -fsS http://localhost:8042/ws/v1/node/info >/dev/null"],
            "interval": "20s", "timeout": "5s", "retries": 15, "start_period": "30s",
        },
        "description": "YARN NodeManager — executes containers on worker node.",
    },

    "hive": {
        "name": "Apache Hive (HiveServer2)",
        "category": "hadoop",
        "image": "apache/hive",
        "default_version": "4.0.1",
        "ports": [10000, 10002],
        "depends_on": ["hive-metastore", "hdfs"],
        "config_roles": ["hive_engine"],
        "env": {
            "SERVICE_NAME": "hiveserver2",
            # Skip re-initializing the schema — bitsondatadev HMS already initialized it
            "SKIP_SCHEMA_INIT": "true",
            "IS_RESUME": "true",
        },
        # apache/hive:4.x entrypoint overwrites hive-site.xml via envsubst — use
        # hiveserver2-site.xml instead, which the entrypoint never touches and HS2
        # reads after hive-site.xml (settings here override the template defaults).
        "volumes": [
            "./config/hive/hiveserver2-site.xml:/opt/hive/conf/hiveserver2-site.xml:ro",
            "./config/hadoop/core-site.xml:/opt/hive/conf/core-site.xml",
        ],
        "healthcheck": {
            "test": ["CMD-SHELL", "bash -c '</dev/tcp/127.0.0.1/10000'"],
            "interval": "30s", "timeout": "15s", "retries": 10, "start_period": "60s",
        },
        "description": "HiveServer2 — SQL interface for HDFS-backed tables via Tez or MapReduce.",
    },

    "tez": {
        "name": "Apache Tez",
        "category": "hadoop",
        "image": "apache/hive",          # Tez ships inside the Hive image; no standalone container
        "default_version": "4.0.0",
        "ports": [],
        "depends_on": ["hdfs", "hadoop-yarn"],
        "config_roles": ["tez_engine"],
        "experimental": True,             # deployed as YARN library, not a long-running service
        "env": {},
        "volumes": [
            "./config/tez/tez-site.xml:/opt/hive/conf/tez-site.xml:ro",
            "./config/hadoop/core-site.xml:/opt/hive/conf/core-site.xml:ro",
        ],
        "description": "Tez DAG engine — fast Hive SQL on YARN. Deployed as HDFS library via post-start init.",
    },

    # ── Security ──────────────────────────────────────────────────────────────

    "ranger-admin": {
        "name": "Apache Ranger Admin",
        "category": "security",
        # The only public Ranger admin image (Ranger 1.1.0). It bakes its DB creds
        # into install.properties pointing at a companion 'postgres-server'
        # (root postgres/security). We repoint it at our shared udp-postgres
        # superuser before startup so it creates its own 'ranger' DB there.
        "image": "wbaa/rokku-dev-apache-ranger",
        "default_version": "latest",
        "ports": ["6080:6080"],
        "depends_on": ["postgres"],
        "config_roles": ["policy_engine"],
        # Rewrite the baked-in DB root creds, then hand off to the image's own
        # entrypoint (which runs setup.sh → ranger-admin on :6080).
        "entrypoint": [
            "sh", "-c",
            "sed -i "
            "'s#^db_host=.*#db_host=udp-postgres:5432#; "
            "s#^db_root_user=.*#db_root_user=lakehouse#; "
            "s#^db_root_password=.*#db_root_password=lakehouse_pass#' "
            "/opt/ranger-1.1.0-admin/install.properties && exec /tmp/entrypoint.sh",
        ],
        "env": {},
        "healthcheck": {
            # Ranger 1.1.0 schema setup + admin boot is slow (~3-5 min) — generous
            # start_period + retries so it settles before being marked unhealthy.
            "test": ["CMD-SHELL", "curl -fsS http://localhost:6080/login.jsp >/dev/null"],
            "interval": "20s", "timeout": "10s", "retries": 40, "start_period": "240s",
        },
        "description": "Centralized authorization (Apache Ranger admin UI) with DB-backed policy store.",
    },

    # ── AI / ML: JupyterLab ──────────────────────────────────────────────────

    "jupyter": {
        "name": "JupyterLab (PySpark + Iceberg)",
        "category": "ai_ml",
        "image": "jupyter/all-spark-notebook",
        "default_version": "spark-3.5.0",
        "ports": ["8889:8888"],
        "depends_on": [],
        "config_roles": ["notebook_server"],
        "env": {
            "JUPYTER_ENABLE_LAB": "yes",
            "GRANT_SUDO": "yes",
            "JUPYTER_TOKEN": "${JUPYTER_TOKEN:-lakehouse}",
            "AWS_ACCESS_KEY_ID": "${MINIO_ROOT_USER:-admin}",
            "AWS_SECRET_ACCESS_KEY": "${MINIO_ROOT_PASSWORD:-udp_admin_12345}",
        },
        "volumes": ["./notebooks:/home/jovyan/work"],
        "healthcheck": {
            "test": ["CMD-SHELL", "curl -fsS http://localhost:8888/api >/dev/null"],
            "interval": "15s", "timeout": "5s", "retries": 12, "start_period": "30s",
        },
        "description": "JupyterLab with PySpark, Iceberg connector, and MinIO S3 integration pre-configured.",
    },

    # ── Catalog: Apache Polaris ───────────────────────────────────────────────

    "polaris": {
        "name": "Apache Polaris",
        "category": "catalog",
        "image": "apache/polaris",
        "default_version": "latest",
        "ports": [8181, 8182],
        "depends_on": ["minio"],
        "config_roles": ["polaris_catalog"],
        "env": {
            "AWS_ACCESS_KEY_ID": "${MINIO_ROOT_USER:-admin}",
            "AWS_SECRET_ACCESS_KEY": "${MINIO_ROOT_PASSWORD:-udp_admin_12345}",
            "AWS_REGION": "us-east-1",
            "POLARIS_BOOTSTRAP_CREDENTIALS": "POLARIS,POLARIS,${MINIO_ROOT_PASSWORD:-udp_admin_12345}",
        },
        "healthcheck": {
            "test": ["CMD-SHELL", "curl -fsS http://localhost:8182/healthcheck >/dev/null"],
            "interval": "15s", "timeout": "5s", "retries": 12, "start_period": "30s",
        },
        "description": "Apache Polaris — multi-engine Iceberg catalog with credential vending and RBAC.",
    },

    # ── Streaming: Debezium CDC ───────────────────────────────────────────────

    "debezium": {
        "name": "Debezium CDC",
        "category": "streaming",
        "image": "quay.io/debezium/connect",
        "default_version": "2.7",
        "ports": [8083],
        "depends_on": ["kafka"],
        "config_roles": ["cdc_ingest"],
        "env": {
            "BOOTSTRAP_SERVERS": "udp-kafka:29092",
            "GROUP_ID": "debezium-connect",
            "CONFIG_STORAGE_TOPIC": "_debezium-configs",
            "OFFSET_STORAGE_TOPIC": "_debezium-offsets",
            "STATUS_STORAGE_TOPIC": "_debezium-status",
            "CONNECT_KEY_CONVERTER": "org.apache.kafka.connect.json.JsonConverter",
            "CONNECT_VALUE_CONVERTER": "org.apache.kafka.connect.json.JsonConverter",
        },
        "healthcheck": {
            "test": ["CMD-SHELL", "curl -fsS http://localhost:8083/connectors >/dev/null"],
            "interval": "20s", "timeout": "5s", "retries": 15, "start_period": "30s",
        },
        "description": "Change Data Capture from operational DBs (PostgreSQL, MySQL, Oracle) into Kafka.",
    },

    # ── Data Lineage: OpenLineage / Marquez ───────────────────────────────────

    "openlineage": {
        "name": "OpenLineage (Marquez)",
        "category": "lineage",
        "image": "marquezproject/marquez",
        "default_version": "latest",
        "ports": [5000, 5001],
        "depends_on": ["postgres"],
        "config_roles": ["lineage_server"],
        "env": {
            "MARQUEZ_PORT": "5000",
            "MARQUEZ_ADMIN_PORT": "5001",
            "POSTGRES_HOST": "udp-postgres",
            "POSTGRES_PORT": "5432",
            "POSTGRES_DB": "${POSTGRES_DB:-lakehouse}",
            "POSTGRES_USER": "${POSTGRES_USER:-lakehouse}",
            "POSTGRES_PASSWORD": "${POSTGRES_PASSWORD:-lakehouse_pass}",
        },
        "healthcheck": {
            "test": ["CMD-SHELL", "curl -fsS http://localhost:5000/api/v1/namespaces >/dev/null"],
            "interval": "20s", "timeout": "5s", "retries": 12, "start_period": "30s",
        },
        "description": "OpenLineage event server (Marquez) — tracks data lineage across Airflow, Spark, dbt.",
    },

    # ── Trino Enterprise (federated SQL across HMS + StarRocks + HDFS) ────────

    "trino-enterprise": {
        "name": "Trino (Enterprise / Federated)",
        "category": "query_engine",
        "image": "trinodb/trino",
        "default_version": "481",
        "ports": ["8285:8080"],
        "depends_on": ["hive-metastore"],
        "config_roles": ["sql_engine", "federated_sql"],
        "env": {
            "JAVA_TOOL_OPTIONS": "${TRINO_JAVA_OPTS:--Xms4G -Xmx4G -XX:+UseG1GC}",
            "TRINO_QUERY_MAX_MEMORY_PER_NODE": "${TRINO_QUERY_MAX_MEMORY_PER_NODE:-2GB}",
        },
        "volumes": ["./config/trino/catalog:/etc/trino/catalog:ro"],
        "healthcheck": {
            "test": ["CMD-SHELL", "curl -fsS http://localhost:8080/v1/info >/dev/null"],
            "interval": "15s", "timeout": "5s", "retries": 30, "start_period": "60s",
        },
        "description": "Federated Trino — query across Hive/Hudi/Delta/Iceberg, StarRocks, HDFS, and RDBMS.",
    },
}

# ---------------------------------------------------------------------------
# Dependency resolution
# ---------------------------------------------------------------------------

# When a component is selected, these are automatically added too.
# Keys can be config_roles or component IDs.
_AUTO_DEPS: dict[str, list[str]] = {
    # Spark variants need an object store. MinIO is the default; if HDFS is
    # already selected the user opted for on-prem storage — skip MinIO auto-add.
    # (resolved by resolve_dependencies which checks existing result set)
    "spark_engine":        ["minio", "minio-create-bucket"],
    # HMS needs MySQL backing DB
    "hive_catalog":        ["mysql-hms"],
    # Nessie/Iceberg REST/Polaris need MinIO
    "nessie_catalog":      ["minio", "minio-create-bucket"],
    "iceberg_rest_catalog":["minio", "minio-create-bucket"],
    "polaris_catalog":     ["minio", "minio-create-bucket"],
    # Hive engine needs YARN for execution
    "hive_engine":         ["hadoop-yarn"],
    # YARN needs NodeManager alongside it
    "yarn_rm":             ["hadoop-yarn-nm"],
    # Tez is a YARN application — needs YARN
    "tez_engine":          ["hadoop-yarn"],
    # Federated Trino benefits from HMS for Hive catalog
    "federated_sql":       ["hive-metastore"],
    # Orchestrators/BI/Transform need Postgres
    "dag_scheduler":       ["postgres"],
    "asset_scheduler":     ["postgres"],
    "bi_tool":             ["postgres"],
    "sql_transform":       ["trino"],
    "lineage_server":      ["postgres"],
    # Ranger needs Postgres for audit (Solr disabled — uses DB audit)
    "policy_engine":       ["postgres"],
    "pg_pool":             ["postgres"],
    # Grafana needs Prometheus
    "dashboard":           ["prometheus"],
    # Kafka needs ZooKeeper; CDC needs Kafka
    "message_bus":         ["zookeeper"],
    "stream_processor":    ["kafka"],
    "cdc_ingest":          ["kafka"],
}

# Mutual exclusions — selecting one removes the other
_MUTUALLY_EXCLUSIVE: list[tuple[str, str]] = [
    ("spark-hudi", "spark-delta"),
    ("spark-hudi", "spark-iceberg"),
    ("spark-delta", "spark-iceberg"),
    ("spark-hudi", "spark"),
    ("spark-delta", "spark"),
    ("spark-iceberg", "spark"),
    ("nessie", "iceberg-rest"),
    ("trino", "trino-enterprise"),   # same image, pick one
    ("debezium", "kafka-connect"),   # both are Kafka Connect — pick one
]

# Display categories for the UI
CATEGORY_GROUPS = {
    "Table Format":   ["spark-iceberg", "spark-hudi", "spark-delta"],
    "Catalog":        ["iceberg-rest", "nessie", "hive-metastore", "polaris"],
    "Storage":        ["minio", "hdfs"],
    "Compute":        ["spark", "spark-hudi", "spark-delta", "spark-iceberg"],
    "Query Engine":   ["trino", "trino-enterprise", "starrocks"],
    "Hadoop":         ["hadoop-yarn", "hive", "tez"],
    "Streaming":      ["kafka", "debezium", "flink", "kafka-connect"],
    "Orchestration":  ["airflow", "dagster"],
    "Transform":      ["dbt"],
    "Lineage":        ["openlineage"],
    "Security":       ["ranger-admin"],
    "Monitoring":     ["prometheus", "grafana", "loki"],
    "BI":             ["superset"],
    "Connection":     ["pgbouncer"],
    "AI / ML":        ["jupyter"],
}


def get_component(comp_id: str) -> dict | None:
    return COMPONENTS.get(comp_id)


def get_live_version(comp_id: str) -> str:
    """Return the best available version for a component.

    Preference order:
      1. version_fetcher live fetch (latest stable from Docker Hub / GitHub)
      2. COMPONENTS[comp_id]["default_version"] (frozen fallback)

    This keeps the registry as a last-resort fallback while letting the system
    track upstream releases without manual edits.
    """
    from . import version_fetcher  # late import to avoid circular at module load

    # version_fetcher uses slightly different IDs for some components:
    # e.g. "spark-hudi" → "spark-hudi", "hive-metastore" → "hive-metastore"
    versions = version_fetcher.get_versions(comp_id)
    good = [v for v in versions if not v.get("error") and v.get("version")]
    if good:
        return good[0]["version"]  # first = latest (fetchers return newest first)
    return COMPONENTS.get(comp_id, {}).get("default_version", "latest")


def get_live_versions(comp_ids: list[str]) -> dict[str, str]:
    """Return {comp_id: latest_version} for a list of component IDs."""
    return {cid: get_live_version(cid) for cid in comp_ids}


def resolve_dependencies(selected: list[str]) -> list[str]:
    """Return the full component list including auto-resolved hard dependencies.

    Hard infrastructure deps (mysql-hms for HMS, postgres for Airflow, etc.)
    are auto-added via _AUTO_DEPS role mappings and component depends_on fields.
    HDFS suppression: when hdfs/hadoop-yarn is in the stack, minio/minio-create-bucket
    are NOT auto-added (user chose on-prem storage).
    """
    result = list(selected)
    changed = True
    while changed:
        changed = False
        additions: list[str] = []
        has_hdfs = any(c in result for c in ("hdfs", "hadoop-yarn", "hadoop-yarn-nm"))
        for cid in list(result):
            comp = COMPONENTS.get(cid)
            if not comp:
                continue
            # Expand explicit depends_on
            for dep in comp.get("depends_on", []):
                if has_hdfs and dep in ("minio", "minio-create-bucket"):
                    continue
                if dep not in result and dep not in additions:
                    additions.append(dep)
            # Expand via config_roles → _AUTO_DEPS
            for role in comp.get("config_roles", []):
                for auto_dep in _AUTO_DEPS.get(role, []):
                    if has_hdfs and auto_dep in ("minio", "minio-create-bucket"):
                        continue
                    if auto_dep not in result and auto_dep not in additions:
                        additions.append(auto_dep)
        if additions:
            result.extend(additions)
            changed = True

    # Stable ordering: infrastructure first, then compute, then query, then extras
    order = [
        # Infrastructure DBs first
        "zookeeper", "postgres", "mysql-hms", "solr",
        # Storage
        "minio", "hdfs", "minio-create-bucket",
        # Catalog
        "hive-metastore", "nessie", "iceberg-rest", "polaris",
        # Hadoop YARN before Hive/Tez
        "hadoop-yarn", "hadoop-yarn-nm",
        # Compute
        "spark", "spark-hudi", "spark-delta", "spark-iceberg",
        "hive", "tez",
        # Streaming
        "kafka", "debezium", "flink", "kafka-connect",
        # Query engines
        "trino", "trino-enterprise", "starrocks",
        # Security
        "ranger-admin",
        # Observability
        "prometheus", "grafana", "loki",
        # Orchestration + Transform + Lineage
        "airflow", "dagster", "dbt", "openlineage",
        # Connection + BI
        "pgbouncer", "superset",
    ]
    ordered = [c for c in order if c in result]
    rest = [c for c in result if c not in ordered]
    return ordered + rest


def get_catalog() -> list[dict]:
    """Return slim catalog entries for the frontend picker."""
    result = []
    for group_name, ids in CATEGORY_GROUPS.items():
        for cid in ids:
            comp = COMPONENTS.get(cid)
            if not comp:
                continue
            result.append({
                "id":          cid,
                "name":        comp["name"],
                "category":    group_name,
                "description": comp.get("description", ""),
                "image":       comp.get("image", ""),
                "version":     comp.get("default_version", ""),
                "experimental": comp.get("experimental", False),
            })
    return result
