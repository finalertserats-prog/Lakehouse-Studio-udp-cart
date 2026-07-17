# Enterprise Hadoop 3.4.1 — matches production techsophy.com dev cluster
# Replaces bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8 (Hadoop 3.2.1 → 3.4.1)
# Compatible with existing hadoop.env (same CORE_CONF_/HDFS_CONF_/YARN_CONF_ convention)
FROM ubuntu:22.04

ARG HADOOP_VERSION=3.4.1

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV HADOOP_HOME=/opt/hadoop-${HADOOP_VERSION}
ENV HADOOP_CONF_DIR=/opt/hadoop-${HADOOP_VERSION}/etc/hadoop
ENV HADOOP_LOG_DIR=/var/log/hadoop
ENV HADOOP_PID_DIR=/var/run/hadoop
ENV PATH=$PATH:/opt/hadoop-${HADOOP_VERSION}/bin:/opt/hadoop-${HADOOP_VERSION}/sbin

# Install Java 11 and minimal utilities
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        openjdk-11-jdk-headless \
        curl wget netcat-openbsd net-tools procps dnsutils \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p ${HADOOP_LOG_DIR} ${HADOOP_PID_DIR}

# Mirror-chain fetcher: tries dlcdn.apache.org first, falls back to archive.apache.org,
# verifies SHA-512 from each mirror's published .sha512 file.
COPY fetch-verified.sh /usr/local/bin/fetch-verified.sh
RUN chmod +x /usr/local/bin/fetch-verified.sh

# Download + install Hadoop (verified against Apache .sha512)
RUN /usr/local/bin/fetch-verified.sh \
        "hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" \
        /tmp/hadoop.tar.gz && \
    tar -xzf /tmp/hadoop.tar.gz -C /opt/ && \
    rm /tmp/hadoop.tar.gz && \
    echo "export JAVA_HOME=${JAVA_HOME}"         >> ${HADOOP_CONF_DIR}/hadoop-env.sh && \
    echo "export HADOOP_LOG_DIR=${HADOOP_LOG_DIR}"  >> ${HADOOP_CONF_DIR}/hadoop-env.sh && \
    echo "export HADOOP_PID_DIR=${HADOOP_PID_DIR}"  >> ${HADOOP_CONF_DIR}/hadoop-env.sh && \
    echo "export YARN_LOG_DIR=${HADOOP_LOG_DIR}"    >> ${HADOOP_CONF_DIR}/yarn-env.sh && \
    echo "export HADOOP_LOG_DIR=${HADOOP_LOG_DIR}"  >> ${HADOOP_CONF_DIR}/yarn-env.sh

# Note: Apache Ranger does NOT publish pre-built plugin binaries on its mirrors —
# only the source tarball (apache-ranger-X.Y.0.tar.gz). Ranger HDFS authorization
# plugin must therefore be built from source. The `ranger-build` Dockerfile in
# this stack already builds the `hdfs-agent` module (alongside Ranger Admin).
#
# To enable HDFS authorization on this image, layer the built plugin via a
# multi-stage COPY from `ehd-ranger-admin:3.0.0-SNAPSHOT` after that image exists,
# or run a post-install step. By default the NameNode runs without Ranger
# enforcement; permission checks use the standard HDFS POSIX ACL pipeline.

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
