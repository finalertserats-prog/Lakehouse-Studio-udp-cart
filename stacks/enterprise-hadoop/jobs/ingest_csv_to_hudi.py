"""
ingest_csv_to_hudi.py — minimal CSV → Hudi ingestion, same shape as
IngestMultipleCsvs._process_table_all_csvs in the live techsophy.com pipeline,
just stripped down to a single table for the Studio demo.

Usage (inside the ehd-spark container, called from the Airflow DAG):

  /opt/spark/bin/spark-submit \
      --master local[2] \
      --jars /opt/spark/extra-jars/hudi-spark3.4-bundle_2.12-1.0.1.jar \
      /home/spark/jobs/ingest_csv_to_hudi.py \
        --input-csv      hdfs://namenode:9820/techsophy/raw/orders_hudi/<file>.csv \
        --hudi-path      hdfs://namenode:9820/techsophy/curated/orders_hudi \
        --table-name     curated_orders \
        --database       default \
        --record-key     order_id \
        --precombine     ts \
        --partition-by   region
"""

from __future__ import annotations
import argparse
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, LongType, StringType, DoubleType,
)


def parse_args(argv):
    p = argparse.ArgumentParser()
    p.add_argument("--input-csv",     required=True)
    p.add_argument("--hudi-path",     required=True)
    p.add_argument("--table-name",    required=True)
    p.add_argument("--database",      default="default")
    p.add_argument("--record-key",    required=True)
    p.add_argument("--precombine",    required=True)
    p.add_argument("--partition-by",  default="")
    p.add_argument("--table-type",    default="COPY_ON_WRITE",
                   choices=["COPY_ON_WRITE", "MERGE_ON_READ"])
    p.add_argument("--write-operation", default="upsert",
                   choices=["upsert", "insert", "bulk_insert"])
    p.add_argument("--mode",          default="append",
                   choices=["append", "overwrite"])
    return p.parse_args(argv)


def main(argv=None):
    args = parse_args(argv or sys.argv[1:])

    # NOTE: deliberately NOT calling .enableHiveSupport() here.  Spark 3.4.4's
    # spark-hive_2.12 was built against Hive 2.3.9; enabling Hive support makes
    # the Hudi writer go through HiveExternalCatalog → HiveUtils, which calls
    # HiveConf.ConfVars.HMSHANDLERINTERVAL — a field that doesn't exist in the
    # Hive 4 jars we've put on the classpath, so it NoSuchFieldErrors.
    # Hudi 1.0.1's own HiveSyncTool (triggered by hoodie.datasource.hive_sync.*
    # options) does the Hive 4 metastore registration separately using the Hive 4
    # client jars staged in /opt/spark/extra-jars/hive4/ — that's the supported path.
    spark = (
        SparkSession.builder
        .appName(f"ingest_csv_to_hudi:{args.table_name}")
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
        .config("spark.sql.catalogImplementation", "in-memory")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # ── read CSV with a fixed demo schema (matches DAG generator) ───────────
    schema = StructType([
        StructField("order_id",  LongType(),   False),
        StructField("order_ref", StringType(), False),
        StructField("region",    StringType(), False),
        StructField("amount",    DoubleType(), False),
        StructField("ts",        LongType(),   False),
    ])

    df = (
        spark.read
        .option("header", "true")
        .schema(schema)
        .csv(args.input_csv)
    )

    # convert epoch seconds → timestamp; keep ts column for precombine
    df = df.withColumn("order_ts", F.from_unixtime(F.col("ts")).cast("timestamp"))

    in_rows = df.count()
    print(f"[ingest] read {in_rows:,} rows from {args.input_csv}")

    # ── Hudi write options ─────────────────────────────────────────────────
    # Hive sync is DISABLED here.  Hudi 1.0.1's HiveSyncTool is compiled against
    # Hive 2.3.9 and statically references HiveConf fields (METASTOREPWD,
    # HMSHANDLERINTERVAL, etc.) that Hive 4 renamed/dropped — so the post-commit
    # metaSync hook crashes with NoSuchFieldError no matter which Hive client
    # jar we put first on the classpath.  The live techsophy.com cluster gets
    # away with it because its Hive 4 metastore was provisioned with backward-
    # compat thrift methods exposed, which the apache/hive:4.0.1 docker image
    # strips.  Until Hudi ships a Hive-4-compatible HiveSyncTool we register
    # the table in HMS via Trino's hudi connector (done in a separate DAG task).
    opts = {
        "hoodie.table.name":                            args.table_name,
        "hoodie.datasource.write.table.type":           args.table_type,
        "hoodie.datasource.write.operation":            args.write_operation,
        "hoodie.datasource.write.recordkey.field":      args.record_key,
        "hoodie.datasource.write.precombine.field":     args.precombine,
        "hoodie.datasource.write.payload.class":        "org.apache.hudi.common.model.DefaultHoodieRecordPayload",
        "hoodie.upsert.shuffle.parallelism":            "2",
        "hoodie.insert.shuffle.parallelism":            "2",
        "hoodie.bulkinsert.shuffle.parallelism":        "2",
        "hoodie.clean.automatic":                       "true",
        "hoodie.clean.policy":                          "KEEP_LATEST_FILE_VERSIONS",
        "hoodie.cleaner.fileversions.retained":         "1",
        "hoodie.metadata.enable":                       "true",
    }

    if args.partition_by:
        opts.update({
            "hoodie.datasource.write.partitionpath.field":     args.partition_by,
            "hoodie.datasource.write.hive_style_partitioning": "true",
            "hoodie.datasource.write.keygenerator.class":      "org.apache.hudi.keygen.ComplexKeyGenerator",
        })
    else:
        opts["hoodie.datasource.write.keygenerator.class"] = "org.apache.hudi.keygen.NonpartitionedKeyGenerator"

    print(f"[ingest] writing Hudi {args.table_type}/{args.write_operation} → {args.hudi_path}")
    (
        df.write
        .format("org.apache.hudi")
        .options(**opts)
        .mode(args.mode)
        .save(args.hudi_path)
    )

    # ── verify by reading the table back through Hudi (proves the write) ──
    # In upsert mode the readback count is cumulative across runs, not equal to
    # this run's input.  We just confirm the table is queryable and has at least
    # this run's rows worth of data.
    rb = spark.read.format("hudi").load(args.hudi_path)
    rb_count = rb.count()
    print(f"[ingest] wrote {in_rows:,} rows this run; total in table = {rb_count}")
    print(f"[ingest] per-region counts:")
    rb.groupBy(args.partition_by or "region").count().orderBy(args.partition_by or "region").show(truncate=False)
    print(f"[ingest] sample rows:")
    rb.select("order_id", "order_ref", "region", "amount", "order_ts").orderBy("order_id").show(5, truncate=False)
    if rb_count < in_rows:
        raise SystemExit(f"readback count {rb_count} < this-run rows {in_rows} — Hudi commit didn't land")
    print(f"[ingest] OK — Hudi table at {args.hudi_path}")
    spark.stop()


if __name__ == "__main__":
    main()
