#!/usr/bin/env bash
# Mirrored from live techsophy.com dev cluster (sdpdevstg01, scanned 2026-05-21).
# Source: /opt/spark-3.4.4-bin-hadoop3/conf/spark-env.sh
# Live keeps this as the stock template (all overrides live in spark-defaults.conf).
# Only HADOOP_CONF_DIR and JAVA_HOME are exported so spark can find the cluster.

export HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-/opt/hadoop-3.4.1/etc/hadoop}
export YARN_CONF_DIR=${YARN_CONF_DIR:-/opt/hadoop-3.4.1/etc/hadoop}
export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-11-openjdk-amd64}
