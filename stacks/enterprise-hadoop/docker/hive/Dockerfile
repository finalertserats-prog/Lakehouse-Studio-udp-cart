# Hive 4.0.1 (Lakehouse Studio enterprise-hadoop stack)
# Extends the official Apache Hive image. Ranger Hive plugin is NOT bundled here —
# see the note in docker/hadoop/Dockerfile. Built-from-source Hive plugin can be
# layered in via a post-install step from the ehd-ranger-admin image.
FROM apache/hive:4.0.1

USER root

# Pre-create the Ranger policycache dir so it's writable if the Hive plugin is
# layered in later. Harmless if Ranger is never enabled.
RUN mkdir -p /etc/ranger/hive/policycache && \
    chown -R hive:hive /etc/ranger/hive/policycache

# Install curl (not present in apache/hive:4.0.1 base image)
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*

# PostgreSQL JDBC driver — not bundled in the official Hive image (defaults to Derby).
# Required for metastore backed by PostgreSQL via PgBouncer.
RUN curl -fsSL "https://jdbc.postgresql.org/download/postgresql-42.7.3.jar" \
        -o /opt/hive/lib/postgresql.jar

COPY entrypoint.sh /hive-ranger-entrypoint.sh
RUN chmod +x /hive-ranger-entrypoint.sh

USER hive
ENTRYPOINT ["/hive-ranger-entrypoint.sh"]
