"""
Stage 1: S3 staging → local HDFS

Mirrors production pushToHDFSFromS3.py pattern exactly.
Differences from production:
  - Reads from S3 but does NOT delete ACK files (read-only on S3)
  - HDFS endpoint points to local Docker namenode (http://namenode:9870)
  - Pushes both CSVs and YAML schema files to local HDFS

Usage:
    python3 01_s3_to_hdfs.py --etlconfig etl_config.yaml --script-home .
    python3 01_s3_to_hdfs.py --etlconfig etl_config.yaml --script-home . --table-names biometric_table
"""
import argparse
import logging
import os
import re
import sys
from collections import defaultdict

import boto3
import yaml
from botocore.exceptions import ClientError
from hdfs import InsecureClient, HdfsError


# ── Logger ────────────────────────────────────────────────────────────────────

class Logger:
    def __init__(self, log_dir: str):
        os.makedirs(log_dir, exist_ok=True)
        self._log = logging.getLogger('s3_to_hdfs')
        self._log.setLevel(logging.DEBUG)

        fmt_detail = '%(asctime)s %(levelname)s %(funcName)s:%(lineno)d — %(message)s'
        fmt_simple = '%(asctime)s %(levelname)s — %(message)s'

        fh = logging.FileHandler(os.path.join(log_dir, 'stage1_s3_to_hdfs.log'))
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(logging.Formatter(fmt_detail))

        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.INFO)
        ch.setFormatter(logging.Formatter(fmt_simple))

        self._log.addHandler(fh)
        self._log.addHandler(ch)

    def info(self, msg):  self._log.info(msg)
    def debug(self, msg): self._log.debug(msg)
    def error(self, msg): self._log.error(msg)


# ── S3 helpers ────────────────────────────────────────────────────────────────

def make_s3_client(cfg: dict):
    return boto3.client(
        's3',
        aws_access_key_id=cfg['access_key_id'],
        aws_secret_access_key=cfg['secret_access_key'],
        region_name=cfg['region_name'],
    )


def list_s3_files(s3, bucket: str, prefix: str, extensions: tuple) -> list:
    """Return all S3 keys under prefix that match any of the given extensions."""
    keys = []
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith(extensions):
                keys.append(obj['Key'])
    return keys


def filter_by_table(keys: list, prefix: str, table_names: str, extensions: tuple) -> list:
    if table_names == '*':
        return keys
    tables = [t.strip() for t in table_names.split(',')]
    return [k for k in keys if any(k[len(prefix):].startswith(t) for t in tables)
            and k.endswith(extensions)]


# ── HDFS helpers ──────────────────────────────────────────────────────────────

def make_hdfs_client(cfg: dict) -> InsecureClient:
    client = InsecureClient(cfg['url'], user=cfg['user'])
    client.status('/')   # validate connection
    return client


def ensure_hdfs_dir(client: InsecureClient, path: str):
    if client.status(path, strict=False) is None:
        client.makedirs(path)


def stream_s3_to_hdfs(s3, bucket: str, key: str,
                       hdfs_client: InsecureClient, hdfs_path: str,
                       log: Logger):
    """Stream an S3 object to HDFS in 8 MB chunks — never loads full file."""
    CHUNK = 8 * 1024 * 1024
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj['Body']
    with hdfs_client.write(hdfs_path, overwrite=True) as writer:
        while True:
            chunk = body.read(CHUNK)
            if not chunk:
                break
            writer.write(chunk)
    log.debug(f'  written → {hdfs_path}')


# ── CSV push ──────────────────────────────────────────────────────────────────

def push_csvs(s3, aws_cfg: dict, hdfs_client: InsecureClient,
              hdfs_csv_path: str, table_names: str, log: Logger):
    bucket = aws_cfg['bucket_name']
    prefix = aws_cfg['csv_s3_path']
    if not prefix.endswith('/'):
        prefix += '/'

    keys = list_s3_files(s3, bucket, prefix, ('.csv',))
    keys = filter_by_table(keys, prefix, table_names, ('.csv',))

    if not keys:
        log.info(f'No CSV files found under s3://{bucket}/{prefix} for tables: {table_names}')
        return 0

    log.info(f'Found {len(keys)} CSV file(s) to push to HDFS')
    ensure_hdfs_dir(hdfs_client, hdfs_csv_path)

    pushed = 0
    for key in keys:
        fname = key.split('/')[-1]
        hdfs_dest = f'{hdfs_csv_path.rstrip("/")}/{fname}'
        try:
            stream_s3_to_hdfs(s3, bucket, key, hdfs_client, hdfs_dest, log)
            # write ACK in HDFS (mirrors production pattern)
            with hdfs_client.write(hdfs_dest + '.ack', overwrite=True) as w:
                w.write(b'')
            pushed += 1
            log.info(f'[OK] {fname}')
        except Exception as exc:
            log.error(f'[FAIL] {fname}: {exc}')
            with hdfs_client.write(hdfs_dest + '.err', overwrite=True) as w:
                w.write(str(exc).encode())

    log.info(f'CSVs pushed: {pushed}/{len(keys)}')
    return pushed


# ── YAML schema push ──────────────────────────────────────────────────────────

def push_yamls(s3, aws_cfg: dict, hdfs_client: InsecureClient,
               hdfs_yaml_path: str, table_names: str, log: Logger):
    bucket = aws_cfg['bucket_name']
    prefix = aws_cfg['yaml_s3_path']
    if not prefix.endswith('/'):
        prefix += '/'

    keys = list_s3_files(s3, bucket, prefix, ('.yaml', '.json'))
    keys = filter_by_table(keys, prefix, table_names, ('.yaml', '.json'))

    if not keys:
        log.info(f'No YAML/JSON schema files found under s3://{bucket}/{prefix}')
        return 0

    log.info(f'Found {len(keys)} schema file(s) to push to HDFS')
    ensure_hdfs_dir(hdfs_client, hdfs_yaml_path)

    pushed = 0
    for key in keys:
        fname = key.split('/')[-1]
        hdfs_dest = f'{hdfs_yaml_path.rstrip("/")}/{fname}'
        try:
            stream_s3_to_hdfs(s3, bucket, key, hdfs_client, hdfs_dest, log)
            # write ACK for schema yaml
            ack_name = fname.replace('_schema.yaml', '_schema.ack')
            with hdfs_client.write(f'{hdfs_yaml_path.rstrip("/")}/{ack_name}', overwrite=True) as w:
                w.write(b'')
            pushed += 1
            log.info(f'[OK] {fname}')
        except Exception as exc:
            log.error(f'[FAIL] {fname}: {exc}')

    log.info(f'YAMLs pushed: {pushed}/{len(keys)}')
    return pushed


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='Stage 1: S3 staging → local HDFS')
    parser.add_argument('--etlconfig',   required=True, help='Path to etl_config.yaml')
    parser.add_argument('--script-home', required=True, help='Directory for logs')
    parser.add_argument('--table-names', default=None,  help='Comma-separated table names, or * for all')
    args = parser.parse_args()

    with open(args.etlconfig) as f:
        cfg = yaml.safe_load(f)

    log_dir = '/tmp/test_raw_ingest_logs'
    log = Logger(log_dir)

    table_names = args.table_names or cfg.get('table_names', '*')
    aws_cfg     = cfg['aws']
    hdfs_cfg    = cfg['hdfs']

    log.info('=== Stage 1: S3 → local HDFS ===')
    log.info(f'Source S3 bucket : {aws_cfg["bucket_name"]}')
    log.info(f'CSV  S3 path     : {aws_cfg["csv_s3_path"]}')
    log.info(f'YAML S3 path     : {aws_cfg["yaml_s3_path"]}')
    log.info(f'HDFS WebHDFS     : {hdfs_cfg["url"]}')
    log.info(f'HDFS CSV  path   : {hdfs_cfg["csv_path"]}')
    log.info(f'HDFS YAML path   : {hdfs_cfg["yaml_path"]}')
    log.info(f'Tables           : {table_names}')

    # S3 client
    try:
        s3 = make_s3_client(aws_cfg)
        s3.head_bucket(Bucket=aws_cfg['bucket_name'])
        log.info('S3 connection OK')
    except ClientError as exc:
        log.error(f'S3 connection failed: {exc}')
        sys.exit(1)

    # HDFS client
    try:
        hdfs_client = make_hdfs_client(hdfs_cfg)
        log.info('HDFS connection OK')
    except Exception as exc:
        log.error(f'HDFS connection failed: {exc}')
        sys.exit(1)

    # Push YAMLs first (Spark ingest needs schema before CSVs)
    yaml_count = push_yamls(s3, aws_cfg, hdfs_client, hdfs_cfg['yaml_path'], table_names, log)

    # Push CSVs
    csv_count = push_csvs(s3, aws_cfg, hdfs_client, hdfs_cfg['csv_path'], table_names, log)

    log.info(f'=== Stage 1 complete — YAMLs: {yaml_count}, CSVs: {csv_count} ===')
    if csv_count == 0:
        log.error('No CSVs pushed — check S3 path and table names')
        sys.exit(1)


if __name__ == '__main__':
    main()
