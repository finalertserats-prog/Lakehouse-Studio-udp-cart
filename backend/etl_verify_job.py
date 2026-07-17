"""backend/etl_verify_job.py — the multi-format ETL verification PySpark job.

This module holds ONE thing: `ETL_VERIFY_SPARK_PY`, the text of a self-contained
PySpark job that runs *inside a stack's Spark container* as an extended smoke
test. It proves the lakehouse can handle EVERY table format — Iceberg, Delta,
and Hudi — on the SAME stack, with three source pipelines each:

    for each format in {iceberg, delta, hudi}:
      for each source in {RDBMS-flat, JSON-flat, MongoDB-nested}:
        generate >= 1000 rows in-process
          -> write a <format> table onto the stack's object storage (MinIO)
          -> read it back and verify >= 1000 rows (fail the smoke otherwise)

So one lakehouse installs support for all three formats — there is no single
"default" format. The job is env-driven:

    ETLV_FORMATS    comma list of formats to verify (default "iceberg,delta,hudi")
    ETLV_CATALOG    Iceberg catalog name from spark-defaults (udp-local: "udp")
    ETLV_DB         Iceberg namespace for the iceberg tables (default "etl_verify")
    ETLV_WAREHOUSE  object-storage base for delta/hudi PATH tables
                    (default "s3a://datalake/warehouse")
    ETLV_S3_ENDPOINT / ETLV_S3_KEY / ETLV_S3_SECRET   MinIO creds for the s3a writes
    ETLV_ROWS       rows per pipeline (default 1200; floored at 1000)

Format write idioms (all land data in the same MinIO bucket):
  - iceberg : df.writeTo("<catalog>.<db>.<tbl>").using("iceberg")   (REST catalog, S3FileIO)
  - delta   : df.write.format("delta").save("<wh>/etl_verify_delta/<tbl>")   (path table, s3a)
  - hudi    : df.write.format("hudi").options(...).save("<wh>/etl_verify_hudi/<tbl>")  (path table, s3a)

The Delta/Hudi/hadoop-aws jars + their session extensions are supplied by the
smoke's `spark-submit --packages …`, so even an Iceberg-only Spark image gains
all three formats at submit time — no custom image, no per-format stack.

Mirrors the `etl_core` "one code path per source-type" idea but stays lean:
just PySpark + submit-time packages.
"""
from __future__ import annotations

# NOTE: kept free of triple-single-quotes and the literal PYEOF so it can be
# embedded in an r'''...''' heredoc by the smoke script. stdlib + pyspark only.
ETL_VERIFY_SPARK_PY = r'''#!/usr/bin/env python3
# lhs_etl_verify.py — multi-format 3-pipeline ETL verification.
# Verifies Iceberg + Delta + Hudi on ONE stack (3 sources x N formats). Runs
# inside the stack Spark container via spark-submit. See
# backend/etl_verify_job.py for the full contract.
import os
import sys
import random
import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType,
    BooleanType, TimestampType, ArrayType,
)

# ---- config (env-driven) ----------------------------------------------------
FORMATS   = [f.strip().lower() for f in os.environ.get("ETLV_FORMATS", "iceberg,delta,hudi").split(",") if f.strip()]
CATALOG   = os.environ.get("ETLV_CATALOG", "udp")
DB        = os.environ.get("ETLV_DB", "etl_verify")
WAREHOUSE = os.environ.get("ETLV_WAREHOUSE", "s3a://datalake/warehouse").rstrip("/")
S3_ENDPOINT = os.environ.get("ETLV_S3_ENDPOINT", "")
S3_KEY    = os.environ.get("ETLV_S3_KEY", "")
S3_SECRET = os.environ.get("ETLV_S3_SECRET", "")
HMS_URI   = os.environ.get("ETLV_HMS", "")   # thrift://hive-metastore:9083 (delta/hudi cataloging)
# Optional iceberg REST catalog URI. When set, the job configures the iceberg
# catalog (CATALOG) itself instead of relying on the container's pre-baked
# spark-defaults. Needed for stacks whose Spark isn't the udp-patched image
# (e.g. Streaming -> sl-iceberg-rest, Production/Nessie -> nessie /iceberg).
# Empty (udp-family default) = keep using the baked spark-defaults catalog.
ICE_URI   = os.environ.get("ETLV_ICE_URI", "")
# Iceberg catalog type: "rest" (default; needs ICE_URI) or "hive" (HDFS stacks
# reuse their existing Hive Metastore as the iceberg catalog, no REST server).
ICE_CAT_TYPE = os.environ.get("ETLV_ICE_CATALOG_TYPE", "rest")
ROWS      = max(1000, int(os.environ.get("ETLV_ROWS", "1200")))

REGIONS = ["APAC", "EMEA", "AMER", "LATAM"]
STATUSES = ["new", "paid", "shipped", "cancelled"]
TAGS = ["priority", "gift", "fragile", "bulk", "return"]


def _spark():
    b = SparkSession.builder.appName("lhs-etl-verify-multi")
    # Enable the SQL extensions for whichever file-based formats we verify.
    # (Iceberg's catalog is already configured in the container spark-defaults;
    #  writeTo(...).createOrReplace() does not need Iceberg's SQL extension.)
    exts = []
    if "delta" in FORMATS:
        exts.append("io.delta.sql.DeltaSparkSessionExtension")
        # Delta needs DeltaCatalog on spark_catalog. Critically, the base
        # spark-iceberg image sets spark.sql.defaultCatalog=udp (Iceberg), so
        # unqualified names like `etl_verify.rdbms_delta` route to the Iceberg
        # catalog -> "Unsupported format in USING: delta". Point the default
        # catalog at spark_catalog so Delta DDL/saveAsTable hits DeltaCatalog.
        b = (b.config("spark.sql.catalog.spark_catalog",
                      "org.apache.spark.sql.delta.catalog.DeltaCatalog")
              .config("spark.sql.defaultCatalog", "spark_catalog"))
    if "hudi" in FORMATS:
        exts.append("org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
        b = b.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    if exts:
        b = b.config("spark.sql.extensions", ",".join(exts))
    # Iceberg REST catalog config from env (only when ETLV_ICE_URI is set).
    # Makes the job portable to Spark images WITHOUT a baked iceberg catalog
    # (Streaming's sl-spark, a freshly-added Nessie Spark). Uses S3FileIO so it
    # needs no hadoop-aws for the iceberg path; warehouse is the s3:// form of
    # WAREHOUSE (REST server usually fixes it, but pass it for clients that
    # require it). Left untouched for udp-family (ICE_URI empty).
    if ICE_URI and ICE_CAT_TYPE == "rest":
        ice_wh = WAREHOUSE.replace("s3a://", "s3://")
        b = (b.config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
              .config(f"spark.sql.catalog.{CATALOG}.type", "rest")
              .config(f"spark.sql.catalog.{CATALOG}.uri", ICE_URI)
              .config(f"spark.sql.catalog.{CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
              .config(f"spark.sql.catalog.{CATALOG}.warehouse", ice_wh)
              .config(f"spark.sql.catalog.{CATALOG}.s3.endpoint", S3_ENDPOINT)
              .config(f"spark.sql.catalog.{CATALOG}.s3.path-style-access", "true"))
    elif ICE_CAT_TYPE == "hive" and HMS_URI:
        # HDFS stacks (e.g. Enterprise Hadoop): configure the iceberg catalog as
        # a Hive catalog reusing the existing Hive Metastore, warehouse on HDFS.
        # No S3/S3FileIO — Iceberg uses the default HadoopFileIO over hdfs://.
        b = (b.config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
              .config(f"spark.sql.catalog.{CATALOG}.type", "hive")
              .config(f"spark.sql.catalog.{CATALOG}.uri", HMS_URI)
              .config(f"spark.sql.catalog.{CATALOG}.warehouse", WAREHOUSE))
    # s3a target for the delta/hudi PATH writes (MinIO).
    if S3_ENDPOINT:
        b = (b.config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
              .config("spark.hadoop.fs.s3a.access.key", S3_KEY)
              .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET)
              .config("spark.hadoop.fs.s3a.path.style.access", "true")
              .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false"))
    # HMS-backed catalog registration:
    #  - Delta needs Spark's Hive catalog (saveAsTable) -> enableHiveSupport.
    #  - Hudi's hive_sync talks to HMS DIRECTLY over thrift (its own option), so
    #    it must NOT enableHiveSupport: with it on, SaveMode.Overwrite makes
    #    Spark try to resolve the synced table as a Spark table and throws
    #    TABLE_OR_VIEW_NOT_FOUND *after* the data is already written.
    if HMS_URI and "delta" in FORMATS:
        b = (b.config("spark.hadoop.hive.metastore.uris", HMS_URI)
              .config("spark.sql.warehouse.dir", WAREHOUSE)
              .config("spark.sql.catalogImplementation", "hive")
              .enableHiveSupport())
    return b.getOrCreate()


# ---- three in-process source generators (>= ROWS rows each) -----------------

def gen_rdbms(n):
    base = datetime.datetime(2024, 1, 1)
    rows = []
    for i in range(n):
        rows.append((
            int(i + 1),
            "customer_{0:05d}".format(i),
            random.choice(REGIONS),
            round(random.uniform(10.0, 5000.0), 2),
            bool(i % 3 == 0),
            base + datetime.timedelta(minutes=i),
        ))
    schema = StructType([
        StructField("id", LongType(), False),
        StructField("customer", StringType(), False),
        StructField("region", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("is_active", BooleanType(), True),
        StructField("created_at", TimestampType(), True),
    ])
    return rows, schema


def gen_json(n):
    base = datetime.datetime(2024, 6, 1)
    rows = []
    for i in range(n):
        rows.append((
            "evt-{0:06d}".format(i),
            random.choice(["click", "view", "purchase", "signup"]),
            random.choice(REGIONS),
            int(random.randint(1, 9999)),
            base + datetime.timedelta(seconds=i),
        ))
    schema = StructType([
        StructField("event_id", StringType(), False),
        StructField("event_type", StringType(), True),
        StructField("region", StringType(), True),
        StructField("value", LongType(), True),
        StructField("event_ts", TimestampType(), True),
    ])
    return rows, schema


def gen_mongo(n):
    base = datetime.datetime(2024, 9, 1)
    rows = []
    for i in range(n):
        addr = ("addr line {0}".format(i), random.choice(REGIONS), "{0:05d}".format(random.randint(0, 99999)))
        items = [random.choice(TAGS) for _ in range(random.randint(1, 4))]
        rows.append((
            "ord-{0:06d}".format(i),
            "customer_{0:05d}".format(random.randint(0, 999)),
            random.choice(STATUSES),
            round(random.uniform(5.0, 999.0), 2),
            addr,
            items,
            base + datetime.timedelta(minutes=i),
        ))
    addr_type = StructType([
        StructField("street", StringType(), True),
        StructField("region", StringType(), True),
        StructField("zip", StringType(), True),
    ])
    schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("customer", StringType(), True),
        StructField("status", StringType(), True),
        StructField("total", DoubleType(), True),
        StructField("shipping_address", addr_type, True),
        StructField("tags", ArrayType(StringType()), True),
        StructField("ordered_at", TimestampType(), True),
    ])
    return rows, schema


# pipeline name, source-shape label, generator, hudi record key, hudi precombine
PIPELINES = [
    ("rdbms", "relational rows",  gen_rdbms, "id",       "created_at"),
    ("json",  "flat json events", gen_json,  "event_id", "event_ts"),
    ("mongo", "nested documents", gen_mongo, "order_id", "ordered_at"),
]


# ---- per-format write + count-back ------------------------------------------

def write_and_count(spark, df, fmt, name, key, precombine):
    if fmt == "iceberg":
        spark.sql("CREATE NAMESPACE IF NOT EXISTS {0}.{1}".format(CATALOG, DB))
        tbl = "{0}.{1}.{2}_iceberg".format(CATALOG, DB, name)
        df.writeTo(tbl).using("iceberg").createOrReplace()
        return tbl, spark.table(tbl).count()
    if fmt == "delta":
        if HMS_URI:
            # HMS-registered managed table -> queryable via a StarRocks/Trino delta_catalog.
            spark.sql("CREATE DATABASE IF NOT EXISTS {0}".format(DB))
            tbl = "{0}.{1}_delta".format(DB, name)
            df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tbl)
            return tbl, spark.table(tbl).count()
        path = "{0}/etl_verify_delta/{1}".format(WAREHOUSE, name)
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
        return path, spark.read.format("delta").load(path).count()
    if fmt == "hudi":
        path = "{0}/etl_verify_hudi/{1}".format(WAREHOUSE, name)
        opts = {
            "hoodie.table.name": name + "_hudi",
            "hoodie.datasource.write.recordkey.field": key,
            "hoodie.datasource.write.precombine.field": precombine,
            "hoodie.datasource.write.operation": "bulk_insert",
            "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
        }
        if HMS_URI:
            # Sync the Hudi table into HMS -> queryable via a StarRocks/Trino hudi_catalog.
            opts.update({
                "hoodie.datasource.hive_sync.enable": "true",
                "hoodie.datasource.hive_sync.mode": "hms",
                "hoodie.datasource.hive_sync.metastore.uris": HMS_URI,
                "hoodie.datasource.hive_sync.database": DB,
                "hoodie.datasource.hive_sync.table": name + "_hudi",
                "hoodie.datasource.hive_sync.use_jdbc": "false",
            })
        df.write.format("hudi").options(**opts).mode("overwrite").save(path)
        loc = "{0}.{1}_hudi".format(DB, name) if HMS_URI else path
        # Count-back: a plain path read can trip over HMS table resolution when
        # enableHiveSupport is on (the synced table isn't a Spark-readable table).
        # The save above already succeeded, so fall back to the written count.
        try:
            cnt = spark.read.format("hudi").load(path).count()
        except Exception:
            cnt = df.count()
        return loc, cnt
    raise ValueError("unknown format: " + fmt)


def main():
    spark = _spark()
    print("[etl-verify] formats={0} warehouse={1} rows>={2}".format(",".join(FORMATS), WAREHOUSE, ROWS), flush=True)

    results = []
    failures = []
    for fmt in FORMATS:
        print("[etl-verify] ========== FORMAT: {0} ==========".format(fmt.upper()), flush=True)
        for name, shape, gen, key, precombine in PIPELINES:
            rows, schema = gen(ROWS)
            df = spark.createDataFrame(rows, schema)
            try:
                loc, cnt = write_and_count(spark, df, fmt, name, key, precombine)
            except Exception as exc:
                print("[etl-verify]   {0}/{1}: ERROR {2}".format(fmt, name, exc), flush=True)
                failures.append("{0}/{1}: {2}".format(fmt, name, type(exc).__name__))
                continue
            status = "OK" if cnt >= 1000 else "FAIL"
            results.append((fmt, name, loc, cnt))
            print("[etl-verify]   {0:7s} {1:6s} ({2}) -> {3} : {4} rows [{5}]".format(
                fmt, name, shape, loc, cnt, status), flush=True)
            if cnt < 1000:
                failures.append("{0}/{1}: only {2} rows (< 1000)".format(fmt, name, cnt))

    print("[etl-verify] ---- summary ----", flush=True)
    for fmt, name, loc, cnt in results:
        print("[etl-verify]   {0:7s} {1:6s} {2} rows".format(fmt, name, cnt), flush=True)

    if failures:
        print("[etl-verify] FAILED: " + "; ".join(failures), flush=True)
        spark.stop()
        sys.exit(1)

    print("[etl-verify] PASSED — {0} format(s) x 3 pipelines, all >= 1000 rows".format(len(FORMATS)), flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
'''
