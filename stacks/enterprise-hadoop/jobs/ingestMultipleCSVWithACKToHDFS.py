import argparse
import logging
import os
import re
import subprocess
import sys
import yaml
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

# In live the script does `import pyarrow.fs as fs` and the JVM-backed
# HadoopFileSystem reaches HDFS via libhdfs.  apache/spark:3.4.4 in our docker
# stack doesn't ship a working libhdfs, so we substitute a WebHDFS-backed shim
# that exposes the same pyarrow.fs API the script consumes.  Everything else in
# the script is unchanged.
import pyarrow_fs_shim as fs  # noqa: F401  (drop-in for pyarrow.fs)
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, to_timestamp, to_date, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType,
    LongType, TimestampType, DateType, BooleanType, DoubleType,
    BinaryType, DecimalType,
)

try:
    import mysql.connector as _mysql_connector
except ImportError:
    _mysql_connector = None

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    stream=sys.stdout,
)


class IngestMultipleCsvs:

    def __init__(self, args):
        self.logger       = logging.getLogger(self.__class__.__name__)
        self.args         = self._create_params_dict(args)
        self.error_tables: List[str] = []
        self.hdfs_client: Optional[fs.HadoopFileSystem] = None

    # ── param normalisation ───────────────────────────────────────────────────

    def _create_params_dict(self, args):
        args['table_names']  = args.get('table_names') or '*'
        args['input_format'] = (args.get('input_format') or 'csv').lower()
        for alias in ('full_load', 'truncate_and_load'):
            if args.get(alias) and not args.get('drop_and_reload'):
                args['drop_and_reload'] = args[alias]
        raw = args.get('drop_and_reload', 'false')
        args['drop_and_reload'] = raw if isinstance(raw, bool) else str(raw).lower() == 'true'
        for key in ('csv_dir', 'parquet_dir', 'yaml_dir'):
            if args.get(key) and not args[key].endswith('/'):
                args[key] += '/'
        return args

    # ── HDFS path helpers ─────────────────────────────────────────────────────

    def _get_path_from_uri(self, uri):
        return urlparse(uri).path if uri.startswith('hdfs://') else uri

    def _validate_hdfs_directory(self, dir_path):
        path = self._get_path_from_uri(dir_path)
        info = self.hdfs_client.get_file_info(path)
        if info.type != fs.FileType.Directory:
            raise ValueError(f'Not a valid directory: {dir_path}')

    # ── Spark ─────────────────────────────────────────────────────────────────

    def _load_spark_session(self) -> SparkSession:
        # NOTE: production uses `.enableHiveSupport()`, but it triggers Spark's
        # bundled Hive 2.3.9 client to call `get_table(dbname, tblname)` against
        # the Hive 4.0.1 metastore — and Hive 4 removed that method from the
        # Thrift IDL (only `get_table_req(GetTableRequest)` remains).  The
        # production cluster appears to have a patched HMS that still exposes
        # `get_table`; apache/hive:4.0.1 docker does not.  We disable Spark Hive
        # support and rely on Hudi's own JDBC sync (via HiveServer2) to create
        # the Hive table.  The Hudi write semantics + Hive registration are
        # otherwise identical to live.
        return (
            SparkSession.builder
            .appName('IngestToHudi')
            .config('spark.yarn.jars',                     '/opt/spark/jars/*.jar')
            .config('spark.sql.extensions',                'org.apache.spark.sql.hudi.HoodieSparkSessionExtension')
            .config('spark.serializer',                    'org.apache.spark.serializer.KryoSerializer')
            .config('spark.kryo.registrator',              'org.apache.spark.HoodieSparkKryoRegistrar')
            .config('spark.sql.warehouse.dir',             self.args['warehouse_dir'])
            .config('spark.sql.debug.maxToStringFields',   '200')
            .config('spark.sql.catalogImplementation',     'in-memory')
            .config('spark.hadoop.fs.permissions.umask-mode', '022')
            .config('spark.hadoop.yarn.resourcemanager.hostname', self.args['yarn_hostname'])
            .config('spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version', '2')
            .config('spark.hadoop.fs.replication',                                  '1')
            .config('spark.sql.adaptive.enabled',                    'true')
            .config('spark.sql.adaptive.coalescePartitions.enabled', 'true')
            .config('spark.sql.files.maxPartitionBytes',             '134217728')
            .getOrCreate()
        )

    # ── shared Hudi write options ─────────────────────────────────────────────

    def _build_hudi_write_params(self, hudi_params: dict, hive_params: dict,
                                  create_hive_table: bool, df_schema_json: str = None) -> dict:
        opts = {
            'hoodie.clean.automatic':                         'true',
            'hoodie.clean.async':                             'false',
            'hoodie.clean.policy':                            'KEEP_LATEST_FILE_VERSIONS',
            'hoodie.cleaner.fileversions.retained':           '1',
            'hoodie.datasource.write.storage.type':           hudi_params['table_type'],
            'hoodie.datasource.write.table.type':             hudi_params['table_type'],
            'hoodie.datasource.write.operation':              hudi_params['write_operation'],
            'hoodie.datasource.write.compression.type':       hudi_params['compression_type'],
            'hoodie.datasource.table.name':                   hudi_params['table_name'],
            'hoodie.table.name':                              hudi_params['table_name'],
            'hoodie.datasource.write.recordkey.field':        hudi_params['primary_key_fields'],
            'hoodie.datasource.write.precombine.field':       hudi_params['precombine_field'],
            'hoodie.datasource.write.payload.class':          'org.apache.hudi.common.model.DefaultHoodieRecordPayload',
            'hoodie.upsert.shuffle.parallelism':              '2',
            'hoodie.insert.shuffle.parallelism':              '2',
            'hoodie.bulkinsert.shuffle.parallelism':          '2',
        }
        if df_schema_json:
            opts['hoodie.datasource.write.schema'] = df_schema_json

        if create_hive_table:
            # Hudi 1.0.1's HiveSyncTool always calls IMetaStoreClient.tableExists()
            # via the removed-in-Hive-4 thrift get_table(String,String) method, so
            # the post-commit hook crashes against the apache/hive:4.0.1 HMS even
            # when mode=jdbc.  Skip Hudi's built-in sync entirely; we register the
            # table ourselves via beeline → HiveServer2 (Hive 4 internal client →
            # get_table_req, the new IDL).
            opts.update({
                'hoodie.datasource.hive_sync.enable': 'false',
                'hoodie.metadata.enable':             'true',
            })

        if hudi_params.get('partition_table', False):
            opts.update({
                'hoodie.datasource.write.partitionpath.field':           hudi_params['partition_fields'],
                'hoodie.datasource.hive_sync.partition_fields':          hudi_params['partition_fields'],
                'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.MultiPartKeysValueExtractor',
                'hoodie.datasource.write.keygenerator.class':            'org.apache.hudi.keygen.ComplexKeyGenerator',
            })
        else:
            opts['hoodie.datasource.write.keygenerator.class'] = 'org.apache.hudi.keygen.NonpartitionedKeyGenerator'

        return opts

    # ── shared: StarRocks refresh ─────────────────────────────────────────────

    def _refresh_starrocks(self, db_name: str, table_name: str) -> None:
        jdbc_url = self.args.get('starrocks_jdbc_url', '')
        if not jdbc_url:
            return
        if _mysql_connector is None:
            print('[StarRocks] mysql-connector-python not installed — skipping refresh')
            return
        try:
            clean      = jdbc_url.replace('jdbc:mysql://', '').split('?')[0].split('/')[0]
            parts      = clean.split(':')
            host, port = parts[0], int(parts[1]) if len(parts) > 1 else 9030
            conn = _mysql_connector.connect(
                host=host, port=port,
                user=self.args.get('starrocks_user', ''),
                password=self.args.get('starrocks_password', ''),
            )
            cur = conn.cursor()
            cur.execute('SET CATALOG hudi_catalog')
            cur.execute(f'REFRESH EXTERNAL TABLE {db_name}.{table_name}')
            cur.close()
            conn.close()
            print(f'[StarRocks] Refreshed {db_name}.{table_name}')
        except Exception as exc:
            print(f'[StarRocks] Warning: refresh failed for {db_name}.{table_name}: {exc}')

    # ── shared: full-reload path clear ───────────────────────────────────────

    def _delete_hudi_path_and_drop_hive_tables(self, spark, hudi_table_uri, hive_db, hive_table):
        path = self._get_path_from_uri(hudi_table_uri)
        try:
            info = self.hdfs_client.get_file_info(path)
            if info.type == fs.FileType.Directory:
                self.hdfs_client.delete_dir(path)
                print(f'Deleted Hudi path: {hudi_table_uri}')
        except FileNotFoundError:
            pass
        except Exception as e:
            print(f'Error clearing Hudi path {hudi_table_uri}: {e}')
            raise

        _ident_re = re.compile(r'^[a-zA-Z0-9_]+$')
        if not _ident_re.match(hive_db) or not _ident_re.match(hive_table):
            raise ValueError(f"Invalid Hive identifier — db='{hive_db}' table='{hive_table}'")
        spark.sql(f'DROP TABLE IF EXISTS `{hive_db}`.`{hive_table}`')
        print(f'Dropped Hive table: {hive_db}.{hive_table}')

    # ══════════════════════════════════════════════════════════════════════════
    # CSV PATH
    # ══════════════════════════════════════════════════════════════════════════

    def _validate_yaml_config_csv(self, yaml_config):
        for key in ('schema', 'hudi', 'create_hive_table', 'hive', 'has_header', 'field_separator', 'null_value_as'):
            if key not in yaml_config:
                raise ValueError(f'CSV YAML missing required key: {key}')
        for key in ('table_path', 'table_name', 'table_type', 'write_operation',
                    'primary_key_fields', 'precombine_field', 'partition_table'):
            if key not in yaml_config['hudi']:
                raise ValueError(f'CSV YAML missing hudi key: {key}')

    def _get_schema_from_yaml(self, config):
        type_map = {
            'string': StringType(), 'integer': IntegerType(), 'long': LongType(),
            'double': DecimalType(), 'float': DecimalType(), 'boolean': BooleanType(),
            'binary': BinaryType(), 'date': DateType(), 'timestamp': TimestampType(),
            'decimal': DecimalType(),
        }
        fields, boolean_fields = [], []
        for field in config['schema']:
            name, dtype, nullable = field['name'], type_map[field['type']], field['nullable']
            if dtype == BooleanType():
                boolean_fields.append(name)
                fields.append(StructField(name, StringType(), nullable))
            elif dtype == DecimalType():
                fields.append(StructField(name, DecimalType(field.get('precision', 11), field.get('scale', 2)), nullable))
            elif dtype == DateType():
                fields.append(StructField(name, dtype, nullable,
                                          metadata={'date_format': field.get('date_format', 'yyyy-MM-dd')}))
            elif dtype == TimestampType():
                fields.append(StructField(name, dtype, nullable,
                                          metadata={'timestamp_format': field.get('date_format', 'yyyy-MM-dd HH:mm:ss')}))
            else:
                fields.append(StructField(name, dtype, nullable))
        return StructType(fields), boolean_fields

    def _fetch_yaml_files(self):
        yaml_dir = self._get_path_from_uri(self.args['yaml_dir'])
        files    = self.hdfs_client.get_file_info(fs.FileSelector(yaml_dir))
        return [f.path for f in files if f.type == fs.FileType.File and f.path.endswith(('.yaml', '.ack'))]

    def _fetch_specific_yaml_files(self, table_names):
        yaml_dir   = self.args['yaml_dir']
        yaml_files, ack_missing, yaml_missing, missing = {}, [], [], []
        for table in [t.strip() for t in table_names.split(',')]:
            yp  = self._get_path_from_uri(f'{yaml_dir}{table}_schema.yaml')
            ap  = self._get_path_from_uri(f'{yaml_dir}{table}_schema.ack')
            yi  = self.hdfs_client.get_file_info(yp)
            ai  = self.hdfs_client.get_file_info(ap)
            has_y, has_a = yi.type == fs.FileType.File, ai.type == fs.FileType.File
            if has_y and has_a:
                yaml_files[table] = f'{yaml_dir}{table}_schema.yaml'
            elif has_y:
                ack_missing.append(table)
            elif has_a:
                yaml_missing.append(table)
            else:
                missing.append(table)
        return yaml_files, ack_missing, yaml_missing, missing

    def _filter_yaml_files(self, files):
        ack_set, yaml_set = set(), set()
        for f in files:
            base = f.split('/')[-1]
            if base.endswith('.yaml'):
                yaml_set.add(base[:-12])
            elif base.endswith('.ack'):
                ack_set.add(base[:-11])
        common = ack_set & yaml_set
        yaml_dir = self.args['yaml_dir']
        return (
            {t: f'{yaml_dir}{t}_schema.yaml' for t in common},
            list(yaml_set - common),
            list(ack_set - common),
            [],
        )

    def _fetch_csv_files(self, yaml_files):
        table_names  = set(yaml_files.keys())
        csv_dir      = self._get_path_from_uri(self.args['csv_dir'])
        file_list    = self.hdfs_client.get_file_info(fs.FileSelector(csv_dir))
        file_groups  = defaultdict(list)
        parent_acks  = set()

        for fi in file_list:
            if fi.type != fs.FileType.File:
                continue
            if not any(fi.base_name.startswith(p) for p in table_names):
                continue
            if fi.base_name.endswith('.ack') and not fi.base_name.endswith('.csv.ack'):
                parent_acks.add(fi.base_name[:-4])
            elif fi.base_name.endswith('.csv'):
                key = re.sub(r'_\d+$', '', fi.base_name[:-4])
                file_groups[key].append(fi)

        for ack in parent_acks:
            if ack not in file_groups:
                file_groups[ack] = []

        err_tables = list(set(file_groups.keys()) - parent_acks)
        return list(parent_acks), err_tables, file_groups

    def _process_table_all_csvs(self, spark, schema, boolean_fields, header, yaml_config, csv_file_infos):
        hudi_params = yaml_config['hudi']
        hive_params = yaml_config.get('hive', {})
        separator   = yaml_config['field_separator'].encode('utf-8').decode('unicode_escape')
        csv_paths   = [fi.path for fi in csv_file_infos]

        df = spark.read.schema(schema).options(
            header=header, sep=separator,
            nullValue=yaml_config['null_value_as'],
            quote='"', escape='"', multiline=True, lineSep='\n',
        ).csv(csv_paths)

        for bf in boolean_fields:
            df = df.withColumn(bf, col(bf).cast(BooleanType()))

        for field in schema.fields:
            if isinstance(field.dataType, TimestampType) and field.name in df.columns:
                df = df.withColumn(field.name, to_timestamp(col(field.name),
                                                            field.metadata.get('timestamp_format', "yyyy-MM-dd'T'HH:mm:ssXXX")))
            elif isinstance(field.dataType, DateType) and field.name in df.columns:
                df = df.withColumn(field.name, to_date(col(field.name),
                                                        field.metadata.get('date_format', 'yyyy-MM-dd')))

        if not hudi_params['primary_key_fields']:
            df = df.withColumn('uuid', F.expr('uuid_compact()'))
            hudi_params['primary_key_fields'] = 'uuid'

        record_key = hudi_params['primary_key_fields']
        before_count = df.count()
        df = df.filter(
            col(record_key).isNotNull() &
            (col(record_key).cast('string') != '') &
            (col(record_key).cast('string') != 'null')
        )
        dropped = before_count - df.count()
        if dropped:
            print(f'[WARNING] Dropped {dropped} row(s) with null/empty record key "{record_key}"')

        if 'timestamp' not in df.columns:
            df = df.withColumn('timestamp', current_timestamp())

        opts = self._build_hudi_write_params(
            hudi_params, hive_params, yaml_config['create_hive_table'], df.schema.json())
        df.write.format('org.apache.hudi').options(**opts).mode('append').save(hudi_params['table_path'])

        if yaml_config.get('create_hive_table'):
            self._register_hive_table_via_beeline(df, hudi_params, hive_params)

    # ── WebHDFS chmod helper ──────────────────────────────────────────────────

    def _webhdfs_chmod_recursive(self, hdfs_uri: str, permission: str = '755') -> None:
        import urllib.request
        host = os.environ.get('WEBHDFS_HOST', 'namenode')
        port = os.environ.get('WEBHDFS_PORT', '9870')
        base = f'http://{host}:{port}/webhdfs/v1'
        path = self._get_path_from_uri(hdfs_uri)

        def _setperm(p):
            url = f'{base}{p}?op=SETPERMISSION&permission={permission}&user.name=spark'
            req = urllib.request.Request(url, method='PUT')
            try:
                urllib.request.urlopen(req, timeout=30)
            except Exception as exc:
                print(f'[hive-register] chmod {p}: {exc}')

        def _list(p):
            url = f'{base}{p}?op=LISTSTATUS&user.name=spark'
            try:
                with urllib.request.urlopen(url, timeout=30) as r:
                    import json as _json
                    return _json.load(r)['FileStatuses']['FileStatus']
            except Exception:
                return []

        _setperm(path)
        for entry in _list(path):
            child = f'{path}/{entry["pathSuffix"]}'
            _setperm(child)
            if entry['type'] == 'DIRECTORY':
                for sub in _list(child):
                    sub_path = f'{child}/{sub["pathSuffix"]}'
                    _setperm(sub_path)

    # ── Hive table registration via beeline (Hive 4 + Hudi 1.0.1 workaround) ──

    def _register_hive_table_via_beeline(self, df, hudi_params, hive_params):
        db   = hive_params['database']
        tbl  = hive_params['table_name']
        path = hudi_params['table_path']
        partition_field = (
            hudi_params['partition_fields'] if hudi_params.get('partition_table') else None
        )

        def spark_to_hive(dtype):
            ts = dtype.simpleString()
            if ts.startswith('decimal('):
                return ts
            return {
                'long': 'bigint', 'integer': 'int', 'short': 'smallint', 'byte': 'tinyint',
                'string': 'string', 'boolean': 'boolean', 'float': 'float', 'double': 'double',
                'timestamp': 'timestamp', 'date': 'date', 'binary': 'binary',
            }.get(ts, ts)

        hudi_cols = [
            '`_hoodie_commit_time`     string',
            '`_hoodie_commit_seqno`    string',
            '`_hoodie_record_key`      string',
            '`_hoodie_partition_path`  string',
            '`_hoodie_file_name`       string',
        ]
        user_cols = [
            f'`{f.name}` {spark_to_hive(f.dataType)}'
            for f in df.schema.fields if f.name != partition_field
        ]
        cols_sql = ',\n  '.join(hudi_cols + user_cols)
        pby_sql  = f"\nPARTITIONED BY (`{partition_field}` string)" if partition_field else ''

        ddl = (
            f"CREATE DATABASE IF NOT EXISTS {db};\n"
            f"CREATE EXTERNAL TABLE IF NOT EXISTS {db}.{tbl} (\n  {cols_sql}\n){pby_sql}\n"
            f"ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'\n"
            f"STORED AS INPUTFORMAT 'org.apache.hudi.hadoop.HoodieParquetInputFormat'\n"
            f"        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'\n"
            f"LOCATION '{path}'\n"
            f"TBLPROPERTIES ('spark.sql.sources.provider'='hudi', 'type'='cow');"
        )

        add_partition_sql = ''
        if partition_field:
            # Get partition values from the DF rather than listing HDFS (the Hudi
            # directory is owned by user `spark` with 750 perms, and our WebHDFS
            # shim queries as user `hadoop` → 403).  The DF only sees the values
            # being written this run; for upserts to existing partitions that's
            # still correct (ALTER ADD IF NOT EXISTS is a no-op).
            rows = df.select(partition_field).distinct().collect()
            parts = sorted({str(r[partition_field]) for r in rows if r[partition_field] is not None})
            if parts:
                specs = '\n  '.join(
                    f"PARTITION (`{partition_field}`='{p}') LOCATION '{path}/{p}'"
                    for p in parts
                )
                add_partition_sql = f"\nALTER TABLE {db}.{tbl} ADD IF NOT EXISTS\n  {specs};"

        sql = ddl + add_partition_sql
        print(f"[hive-register] DDL for {db}.{tbl}:\n{sql}")

        # Hive 4 HMS validates partition locations via HDFS stat (as user=hive).
        # New writes use umask 022 (644/755), but existing paths written under the
        # old 027 umask need a one-time fix so hive/trino can traverse them.
        self._webhdfs_chmod_recursive(path)

        res = subprocess.run(
            ['/opt/spark/bin/beeline',
             '-u', 'jdbc:hive2://hiveserver2:10000/',
             '-n', 'hive',
             '--silent=false',
             '--showHeader=false',
             '-e', sql],
            capture_output=True, text=True, timeout=180,
        )
        if res.stdout:
            print(f"[hive-register] stdout:\n{res.stdout}")
        if res.returncode != 0:
            print(f"[hive-register] stderr:\n{res.stderr}")
            raise RuntimeError(f"beeline DDL failed (rc={res.returncode}) for {db}.{tbl}")
        print(f"[hive-register] OK — {db}.{tbl} registered")

    def _run_csv(self, spark):
        if self.args['table_names'] == '*':
            raw_files = self._fetch_yaml_files()
            yaml_files, ack_missing, yaml_missing, missing = self._filter_yaml_files(raw_files)
        else:
            yaml_files, ack_missing, yaml_missing, missing = self._fetch_specific_yaml_files(self.args['table_names'])

        if not yaml_files:
            print('No YAML files found for processing.')
            return False

        csv_tables, err_tables, grp_files = self._fetch_csv_files(yaml_files)

        delta_csvs = '/delta/' in self.args['csv_dir']

        for table in csv_tables:
            yaml_key  = '_'.join(table.split('_')[:-2]) if delta_csvs else table
            yaml_file = yaml_files.get(yaml_key)
            if not yaml_file:
                self.error_tables.append(table)
                continue
            try:
                yaml_path = self._get_path_from_uri(yaml_file)
                with self.hdfs_client.open_input_stream(yaml_path) as fh:
                    yaml_config = yaml.safe_load(fh.read())
                self._validate_yaml_config_csv(yaml_config)

                hudi_path  = yaml_config['hudi']['table_path']
                hive_db    = yaml_config['hive']['database']
                hive_table = yaml_config['hive']['table_name']

                if self.args.get('drop_and_reload'):
                    self._delete_hudi_path_and_drop_hive_tables(spark, hudi_path, hive_db, hive_table)

                csv_files_info = grp_files[table]
                if not csv_files_info:
                    self.error_tables.append(table)
                    print(f'[{table}] No CSV files found — skipping.')
                    continue

                schema, boolean_fields = self._get_schema_from_yaml(yaml_config)
                header = 'true' if yaml_config.get('has_header', True) else 'false'

                self._process_table_all_csvs(spark, schema, boolean_fields, header, yaml_config, csv_files_info)

                # Delete per-file ACKs after successful batch write
                for fi in csv_files_info:
                    ack_path = self._get_path_from_uri(fi.path + '.ack')
                    try:
                        self.hdfs_client.delete_file(ack_path)
                    except FileNotFoundError:
                        pass

                # Delete parent ACK
                parent_ack = self._get_path_from_uri(f"{self.args['csv_dir']}{table}.ack")
                try:
                    self.hdfs_client.delete_file(parent_ack)
                except FileNotFoundError:
                    pass
                except Exception as e:
                    print(f'[{table}] Warning: could not delete parent ACK: {e}')

                self._refresh_starrocks(hive_db, hive_table)

            except Exception as ex:
                print(f'[{table}] Error: {ex}')
                parent_ack = self._get_path_from_uri(f"{self.args['csv_dir']}{table}.ack")
                try:
                    self.hdfs_client.delete_file(parent_ack)
                except Exception:
                    pass
                self.error_tables.append(table)

        errs = len(self.error_tables) + len(err_tables) + len(ack_missing) + len(yaml_missing) + len(missing)
        if errs:
            print(f'Finished with {errs} errors.')
            print(f'  processing errors: {self.error_tables}')
            print(f'  parent ack errors: {err_tables}')
            print(f'  missing ack:       {ack_missing}')
            print(f'  missing yaml:      {yaml_missing}')
            print(f'  other missing:     {missing}')
            return False
        print('All CSV tables processed successfully.')
        return True

    # ══════════════════════════════════════════════════════════════════════════
    # ENTRYPOINT
    # ══════════════════════════════════════════════════════════════════════════

    def run(self):
        input_format = self.args['input_format']

        try:
            hdfs_uri         = self.args['hdfs_uri']
            self.hdfs_client = fs.HadoopFileSystem.from_uri(hdfs_uri)

            if input_format == 'csv':
                self._validate_hdfs_directory(self.args['yaml_dir'])
                self._validate_hdfs_directory(self.args['csv_dir'])
            else:
                raise ValueError(f"Only 'csv' input_format supported in this slim build.")
            spark            = self._load_spark_session()
            result = self._run_csv(spark)
            spark.stop()
            return result

        except Exception as ex:
            print(f'Unhandled exception during run: {ex}')
            return False


# ── CLI ───────────────────────────────────────────────────────────────────────

def arg_parser():
    p = argparse.ArgumentParser(
        description='Ingest CSV (ACK-based) into Hudi'
    )
    p.add_argument('--input_format',     default='csv', choices=['csv'])
    p.add_argument('--yaml_dir',         required=True)
    p.add_argument('--hdfs_uri',         required=True)
    p.add_argument('--yarn_hostname',    required=True)
    p.add_argument('--warehouse_dir',    required=True)
    p.add_argument('--csv_dir',          required=True)
    p.add_argument('--table_names',      default='*')
    p.add_argument('--drop_and_reload',  default='')
    p.add_argument('--starrocks_jdbc_url', default='')
    p.add_argument('--starrocks_user',     default='')
    p.add_argument('--starrocks_password', default='')
    return vars(p.parse_args())


def main():
    args      = arg_parser()
    processor = IngestMultipleCsvs(args)
    return processor.run()


if __name__ == '__main__':
    main()
