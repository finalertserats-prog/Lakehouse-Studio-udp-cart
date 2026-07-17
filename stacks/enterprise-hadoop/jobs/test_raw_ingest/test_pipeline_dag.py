"""
Airflow DAG — test raw ingest pipeline
Mirrors production master_techsophy_biometric_postgres_ingest_raw_dag.py pattern.

Stages:
  1. s3_to_hdfs       : BashOperator — pulls CSVs + YAML schemas from S3 into local HDFS
  2. ingest_to_hudi   : BashOperator — spark-submit inside ehd-spark container

Production differences:
  - No SSHOperator (spark runs in local Docker via docker exec)
  - HDFS = namenode:9820, S3 source = same gayatri2datalake bucket
  - schedule_interval = None (trigger manually)
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

SCRIPT_DIR  = '/opt/airflow/jobs/test_raw_ingest'
ETL_CONFIG  = f'{SCRIPT_DIR}/etl_config.yaml'
HUDI_JAR    = '/tmp/spark-bootstrap/hudi-spark3.4-bundle_2.12-1.0.1.jar'
SPARK_BIN   = '/opt/spark/bin/spark-submit'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='test_raw_ingest_pipeline',
    default_args=default_args,
    description='Test: S3 biometric staging → local HDFS → Hudi CoW raw table',
    schedule_interval=None,
    catchup=False,
    tags=['test', 'raw', 'hudi', 'biometric'],
) as dag:

    start = BashOperator(
        task_id='start',
        bash_command='echo "Test raw ingest pipeline started at $(date)"',
    )

    # Stage 1: S3 → local HDFS  (runs in Airflow container)
    s3_to_hdfs = BashOperator(
        task_id='s3_to_hdfs',
        bash_command=(
            f'pip install boto3 hdfs pyyaml --quiet && '
            f'python3 {SCRIPT_DIR}/01_s3_to_hdfs.py '
            f'--etlconfig {ETL_CONFIG} '
            f'--script-home {SCRIPT_DIR}'
        ),
        execution_timeout=None,
    )

    # Stage 2: HDFS → Hudi  (runs inside ehd-spark container via docker exec)
    ingest_to_hudi = BashOperator(
        task_id='ingest_to_hudi',
        bash_command=(
            'docker exec ehd-spark '
            f'{SPARK_BIN} '
            '--master local[2] '
            f'--jars {HUDI_JAR} '
            f'/tmp/test_ingest/02_ingest_to_hudi.py '
            '--csv_dir  /techsophy/raw/test/biometric/csvs/ '
            '--yaml_dir /techsophy/raw/test/biometric/yamls/ '
            '--hdfs_uri hdfs://namenode:9820 '
            '--warehouse_dir /tmp/hive/warehouse '
            '--yarn_hostname resourcemanager'
        ),
        execution_timeout=None,
    )

    done = BashOperator(
        task_id='done',
        bash_command='echo "Test raw ingest pipeline completed at $(date)"',
    )

    start >> s3_to_hdfs >> ingest_to_hudi >> done
