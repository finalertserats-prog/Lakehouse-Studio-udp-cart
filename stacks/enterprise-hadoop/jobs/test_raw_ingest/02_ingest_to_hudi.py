"""
Stage 2: local HDFS CSV → Hudi CoW table via PySpark

Mirrors production ingestMultipleCSVWithACKToHDFS.py exactly.
Differences from production:
  - spark.master = local[2]  (no YARN cluster needed)
  - hdfs_uri     = hdfs://namenode:9820  (local Docker NameNode)
  - Hudi JAR path = /tmp/spark-bootstrap/hudi-spark3.4-bundle_2.12-1.0.1.jar

Run via spark-submit inside ehd-spark container:
    docker exec ehd-spark spark-submit \\
        --master local[2] \\
        --jars /tmp/spark-bootstrap/hudi-spark3.4-bundle_2.12-1.0.1.jar \\
        /tmp/test_ingest/02_ingest_to_hudi.py \\
        --csv_dir  /techsophy/raw/test/biometric/csvs/ \\
        --yaml_dir /techsophy/raw/test/biometric/yamls/ \\
        --hdfs_uri hdfs://namenode:9820 \\
        --warehouse_dir /tmp/hive/warehouse \\
        --yarn_hostname resourcemanager
"""
import argparse
import re
import sys
import yaml
from collections import defaultdict

import pyarrow.fs as fs
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BinaryType, BooleanType, DateType, DecimalType, DoubleType,
    FloatType, IntegerType, LongType, StringType, StructField, StructType,
    TimestampType,
)


# ── SparkSession ──────────────────────────────────────────────────────────────

def build_spark(hdfs_uri: str, warehouse_dir: str, yarn_hostname: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName('TestRawIngest_CSVToHudi')
        .config('spark.sql.extensions',       'org.apache.spark.sql.hudi.HoodieSparkSessionExtension')
        .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.hudi.catalog.HoodieCatalog')
        .config('spark.kryo.registrator',     'org.apache.spark.HoodieSparkKryoRegistrar')
        .config('spark.serializer',           'org.apache.spark.serializer.KryoSerializer')
        .config('spark.sql.warehouse.dir',    warehouse_dir)
        .config('spark.sql.debug.maxToStringFields', '200')
        .config('spark.hadoop.fs.defaultFS',  hdfs_uri)
        .config('spark.hadoop.yarn.resourcemanager.hostname', yarn_hostname)
        .config('spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version', '2')
        .config('spark.hadoop.fs.replication', '1')
        .enableHiveSupport()
        .getOrCreate()
    )


# ── Schema helpers ────────────────────────────────────────────────────────────

_TYPE_MAP = {
    'string':    StringType(),
    'integer':   IntegerType(),
    'long':      LongType(),
    'double':    DoubleType(),
    'float':     FloatType(),
    'boolean':   BooleanType(),
    'binary':    BinaryType(),
    'date':      DateType(),
    'timestamp': TimestampType(),
    'decimal':   DecimalType(),
}


def schema_from_yaml(cfg: dict) -> StructType:
    fields = []
    for f in cfg['schema']:
        dt = _TYPE_MAP.get(f['type'], StringType())
        if f['type'] == 'decimal':
            dt = DecimalType(f.get('precision', 18), f.get('scale', 4))
        fields.append(StructField(f['name'], dt, f.get('nullable', True)))
    return StructType(fields)


# ── HDFS file listing ─────────────────────────────────────────────────────────

def list_hdfs_files(hdfs_client, directory: str, exts: tuple) -> list:
    sel = fs.FileSelector(directory)
    return [f for f in hdfs_client.get_file_info(sel)
            if f.type == fs.FileType.File and f.path.endswith(exts)]


def find_yaml_files(hdfs_client, yaml_dir: str) -> dict:
    """Return {table_name: yaml_path} for tables that have both .yaml and .ack."""
    files = list_hdfs_files(hdfs_client, yaml_dir, ('.yaml', '.ack'))
    acks, yamls = set(), {}
    for f in files:
        name = f.path.split('/')[-1]
        if name.endswith('_schema.ack'):
            acks.add(name[:-11])
        elif name.endswith('_schema.yaml'):
            yamls[name[:-12]] = f.path
    return {t: yamls[t] for t in acks & set(yamls)}


def find_csv_files(hdfs_client, csv_dir: str, table_names: set) -> tuple:
    """Return (ready_tables, err_tables, {table: [FileInfo]})."""
    files = list_hdfs_files(hdfs_client, csv_dir, ('.csv', '.ack'))
    groups: dict = defaultdict(list)
    parent_acks: set = set()

    for f in files:
        base = f.path.split('/')[-1]
        if base.endswith('.ack') and not base.endswith('.csv.ack'):
            parent_acks.add(base[:-4])
        elif base.endswith('.csv'):
            tbl = re.sub(r'_\d+$', '', base[:-4])
            if tbl in table_names:
                groups[tbl].append(f)

    for t in parent_acks:
        if t not in groups:
            groups[t] = []

    err_tables = list(set(groups) - parent_acks)
    ready = list(parent_acks)
    return ready, err_tables, groups


# ── Ingest one table ──────────────────────────────────────────────────────────

def ingest_table(spark: SparkSession, hdfs_client, schema: StructType,
                 yaml_cfg: dict, csv_files: list, hdfs_uri: str):
    hudi = yaml_cfg['hudi']
    hive = yaml_cfg.get('hive', {})
    sep  = yaml_cfg.get('field_separator', '\t').encode('utf-8').decode('unicode_escape')
    header = 'true' if yaml_cfg.get('has_header', False) else 'false'
    null_val = yaml_cfg.get('null_value_as', r'\N')

    # Adjust table_path to point to local HDFS
    table_path = hudi['table_path']
    if 'sdpdevnn01' in table_path or 'sdpdevstg01' in table_path:
        # rewrite production HDFS URI to local
        table_path = re.sub(r'hdfs://[^/]+', hdfs_uri, table_path)
        # rewrite production path zones to test zone
        table_path = table_path.replace('/techsophy/raw/', '/techsophy/raw/test/')

    write_params = {
        'hoodie.datasource.write.storage.type':    hudi['table_type'],
        'hoodie.datasource.write.table.type':      hudi['table_type'],
        'hoodie.datasource.write.operation':       hudi['write_operation'],
        'hoodie.datasource.write.compression.type': hudi.get('compression_type', 'GZIP'),
        'hoodie.datasource.table.name':            hudi['table_name'],
        'hoodie.table.name':                       hudi['table_name'],
        'hoodie.datasource.write.recordkey.field': hudi['primary_key_fields'],
        'hoodie.datasource.write.precombine.field': hudi['precombine_field'],
        'hoodie.datasource.write.keygenerator.class': (
            'org.apache.hudi.keygen.NonpartitionedKeyGenerator'
            if not hudi.get('partition_table', False)
            else 'org.apache.hudi.keygen.ComplexKeyGenerator'
        ),
    }

    if yaml_cfg.get('create_hive_table') and hive:
        write_params.update({
            'hoodie.datasource.hive_sync.enable':      'true',
            'hoodie.datasource.hive_sync.table':       hive.get('table_name', hudi['table_name']),
            'hoodie.datasource.hive_sync.database':    hive.get('database', 'default'),
            'hoodie.datasource.hive_sync.auto_schemaprovider': 'false',
            'hoodie.datasource.hive_sync.schema_evolution':    'false',
        })

    if hudi.get('partition_table', False):
        write_params.update({
            'hoodie.datasource.write.partitionpath.field':       hudi['partition_fields'],
            'hoodie.datasource.hive_sync.partition_fields':      hudi['partition_fields'],
            'hoodie.datasource.hive_sync.partition_extractor_class':
                'org.apache.hudi.hive.MultiPartKeysValueExtractor',
        })

    for csv_file in csv_files:
        df = spark.read.schema(schema).options(
            header=header, sep=sep, nullValue=null_val
        ).csv(csv_file.path)

        df = spark.createDataFrame(df.rdd, schema)
        df.show(5, truncate=True)

        df.write.format('org.apache.hudi') \
            .options(**write_params) \
            .mode(hudi.get('save_mode', 'append')) \
            .save(table_path)

        print(f'[OK] {csv_file.path} → {table_path}')

        # delete .csv.ack
        ack = csv_file.path + '.ack'
        try:
            hdfs_client.delete_file(ack)
        except Exception:
            pass

    # delete parent table.ack
    parent_ack = f'{csv_file.path.rsplit("/", 1)[0]}/{csv_file.path.split("/")[-1][:-len(csv_file.path.split("/")[-1])]}'


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='Stage 2: HDFS CSV → Hudi')
    parser.add_argument('--csv_dir',      required=True)
    parser.add_argument('--yaml_dir',     required=True)
    parser.add_argument('--hdfs_uri',     required=True)
    parser.add_argument('--warehouse_dir',required=True)
    parser.add_argument('--yarn_hostname',required=True)
    parser.add_argument('--table_names',  default='*')
    args = parser.parse_args()

    hdfs_client = fs.HadoopFileSystem.from_uri(args.hdfs_uri)

    csv_dir  = args.csv_dir.rstrip('/')  + '/'
    yaml_dir = args.yaml_dir.rstrip('/') + '/'

    print(f'HDFS URI   : {args.hdfs_uri}')
    print(f'CSV  dir   : {csv_dir}')
    print(f'YAML dir   : {yaml_dir}')

    # Find schema YAML files
    yaml_files = find_yaml_files(hdfs_client, yaml_dir)
    if not yaml_files:
        print('ERROR: No schema YAML files found in HDFS yaml_dir. Run stage 1 first.')
        sys.exit(1)
    print(f'Schema files found: {list(yaml_files.keys())}')

    # Filter to requested tables
    if args.table_names != '*':
        requested = {t.strip() for t in args.table_names.split(',')}
        yaml_files = {t: p for t, p in yaml_files.items() if t in requested}

    # Find CSV files
    ready_tables, err_tables, csv_groups = find_csv_files(
        hdfs_client, csv_dir, set(yaml_files.keys())
    )
    if err_tables:
        print(f'WARN: tables without parent ACK (skipped): {err_tables}')
    if not ready_tables:
        print('ERROR: No ready CSV files (need parent .ack). Run stage 1 first.')
        sys.exit(1)

    print(f'Tables ready to ingest: {ready_tables}')

    spark = build_spark(args.hdfs_uri, args.warehouse_dir, args.yarn_hostname)

    errors = []
    for table in ready_tables:
        yaml_path  = yaml_files[table]
        csv_files  = csv_groups[table]

        try:
            with hdfs_client.open_input_stream(yaml_path) as fh:
                yaml_cfg = yaml.safe_load(fh.read())

            schema = schema_from_yaml(yaml_cfg)
            print(f'\n--- Ingesting {table} ({len(csv_files)} file(s)) ---')
            ingest_table(spark, hdfs_client, schema, yaml_cfg, csv_files, args.hdfs_uri)
        except Exception as exc:
            print(f'[FAIL] {table}: {exc}')
            errors.append(table)

    spark.stop()

    if errors:
        print(f'\nFailed tables: {errors}')
        sys.exit(1)
    print('\n=== Stage 2 complete ===')


if __name__ == '__main__':
    main()
