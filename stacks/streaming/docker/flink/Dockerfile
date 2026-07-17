FROM flink:1.20.1-scala_2.12-java11

USER root

# Iceberg Flink runtime (Flink 1.20 requires Iceberg 1.7+; 1.6.x only shipped 1.18/1.19 runtimes)
RUN wget -q \
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.20/1.8.1/iceberg-flink-runtime-1.20-1.8.1.jar" \
    -O /opt/flink/lib/iceberg-flink-runtime-1.20-1.8.1.jar

# Iceberg AWS bundle — provides S3FileIO + AWS SDK v2 so the Flink Iceberg
# sink writes s3:// paths natively (same FileIO Spark uses; no Hadoop S3A).
RUN wget -q \
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.8.1/iceberg-aws-bundle-1.8.1.jar" \
    -O /opt/flink/lib/iceberg-aws-bundle-1.8.1.jar

# Kafka SQL connector for Flink 1.20 (bundled version — includes kafka-clients)
# flink-connector-kafka is the API-only jar; flink-sql-connector-kafka bundles everything
RUN wget -q \
    "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar" \
    -O /opt/flink/lib/flink-sql-connector-kafka-3.3.0-1.20.jar

# S3 / MinIO filesystem plugin (presto client — fast, no Hadoop overhead)
RUN mkdir -p /opt/flink/plugins/s3-fs-presto && \
    wget -q \
    "https://repo1.maven.org/maven2/org/apache/flink/flink-s3-fs-presto/1.20.1/flink-s3-fs-presto-1.20.1.jar" \
    -O /opt/flink/plugins/s3-fs-presto/flink-s3-fs-presto-1.20.1.jar

# iceberg-flink-runtime needs Hadoop's Configuration and related classes at
# catalog-init time, but flink-s3-fs-hadoop ships as a Flink plugin (isolated
# classloader) that also triggers HDFS registration when placed in lib/.
# Fix: copy the fat JAR's Hadoop classes into lib/ WITHOUT the Flink plugin
# ServiceLoader entry so the Hadoop classes are visible but no plugin init runs.
# The flink base image ships no python3 — install it first (JRE has no `jar` tool).
RUN apt-get update && apt-get install -y --no-install-recommends python3 && rm -rf /var/lib/apt/lists/*
RUN python3 <<'EOF'
import zipfile
skip = ('META-INF/services/org.apache.flink', 'org/apache/flink')
src = '/opt/flink/opt/flink-s3-fs-hadoop-1.20.1.jar'
dst = '/opt/flink/lib/hadoop-classes.jar'
with zipfile.ZipFile(src,'r') as zi, zipfile.ZipFile(dst,'w',zipfile.ZIP_DEFLATED) as zo:
    for it in zi.infolist():
        if not any(it.filename.startswith(p) for p in skip):
            zo.writestr(it, zi.read(it.filename))
print('hadoop-classes.jar built')
EOF

# hadoop-hdfs-client provides HdfsConfiguration, which Hadoop's Configuration class
# tries to load when initializing the HDFS filesystem entry in core-default.xml.
RUN wget -q \
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-hdfs-client/3.3.4/hadoop-hdfs-client-3.3.4.jar" \
    -O /opt/flink/lib/hadoop-hdfs-client-3.3.4.jar


USER flink
