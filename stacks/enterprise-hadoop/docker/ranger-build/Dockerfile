# Apache Ranger 3.0.0-SNAPSHOT — built from Apache GitHub master branch
# Matches production: techsophy.com dev cluster (sdpdevnn01 /usr/lib/ranger/)
#
# Multi-stage build:
#   Stage 1: Maven build (JDK 11 + Maven 3.9) — downloads all deps, compiles Ranger
#   Stage 2: Runtime image — only the built component tarball
#
# Build time: 30-60 min on first run (Maven downloads ~1GB of deps).
# Subsequent builds use Docker layer cache and take ~2 min.
#
# Usage:
#   Build admin image:
#     docker build --target ranger-admin \
#       -t ehd-ranger-admin:3.0.0-SNAPSHOT \
#       docker/ranger-build/
#   Build usersync image:
#     docker build --target ranger-usersync \
#       -t ehd-ranger-usersync:3.0.0-SNAPSHOT \
#       docker/ranger-build/

# ──────────────────────────────────────────────────────────────────────────────
# Stage 1: Build Ranger 3.0.0-SNAPSHOT from Apache GitHub
# ──────────────────────────────────────────────────────────────────────────────
FROM maven:3.9.6-eclipse-temurin-17 AS ranger-builder

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        git unzip python3 \
    && rm -rf /var/lib/apt/lists/*

# Clone Apache Ranger master branch (3.0.0-SNAPSHOT)
RUN git clone --depth=1 \
        https://github.com/apache/ranger.git \
        /ranger-src

WORKDIR /ranger-src

# Build required components using directory module names (master branch layout):
#   security-admin  → Ranger Admin
#   ugsync          → User Sync
#   hdfs-agent      → HDFS plugin
#   hive-agent      → Hive plugin
# -am pulls in all upstream dependencies automatically
RUN mvn clean package \
        -DskipTests \
        -Drat.skip=true \
        -Dpdm.skip=true \
        -Dcheckstyle.skip=true \
        -pl security-admin,ugsync,hdfs-agent,hive-agent \
        -am \
        -T 1 2>&1 | tee /tmp/ranger-build.log | tail -50

# Copy built tarballs to a staging area for the runtime stages
# Each module writes its tarball under its own target/ directory
RUN mkdir -p /ranger-dist && \
    find /ranger-src/security-admin/target -name "ranger-*-admin.tar.gz"       -exec cp {} /ranger-dist/ \; 2>/dev/null; \
    find /ranger-src/ugsync/target         -name "ranger-*-usersync.tar.gz"    -exec cp {} /ranger-dist/ \; 2>/dev/null; \
    find /ranger-src/hdfs-agent/target     -name "ranger-*-hdfs-plugin.tar.gz" -exec cp {} /ranger-dist/ \; 2>/dev/null; \
    find /ranger-src/hive-agent/target     -name "ranger-*-hive-plugin.tar.gz" -exec cp {} /ranger-dist/ \; 2>/dev/null; \
    find /ranger-src -name "ranger-*-admin.tar.gz"       -not -path "*/ranger-dist/*" -exec cp {} /ranger-dist/ \; 2>/dev/null; \
    find /ranger-src -name "ranger-*-usersync.tar.gz"    -not -path "*/ranger-dist/*" -exec cp {} /ranger-dist/ \; 2>/dev/null; \
    find /ranger-src -name "ranger-*-hdfs-plugin.tar.gz" -not -path "*/ranger-dist/*" -exec cp {} /ranger-dist/ \; 2>/dev/null; \
    find /ranger-src -name "ranger-*-hive-plugin.tar.gz" -not -path "*/ranger-dist/*" -exec cp {} /ranger-dist/ \; 2>/dev/null; \
    ls -lh /ranger-dist/

# ──────────────────────────────────────────────────────────────────────────────
# Stage 2a: Ranger Admin runtime
# ──────────────────────────────────────────────────────────────────────────────
FROM ubuntu:22.04 AS ranger-admin

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV RANGER_HOME=/opt/ranger/admin

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        openjdk-17-jdk-headless python3 python3-pip \
        curl wget postgresql-client netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ranger-builder /ranger-dist/ranger-*-admin.tar.gz /tmp/ranger-admin.tar.gz
RUN mkdir -p /opt/ranger/admin && \
    tar -xzf /tmp/ranger-admin.tar.gz -C /opt/ranger/admin --strip-components=1 && \
    rm /tmp/ranger-admin.tar.gz

RUN wget -q \
        "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar" \
        -O /opt/ranger/admin/ews/lib/postgresql-42.7.3.jar

COPY entrypoint-admin.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 6080
ENTRYPOINT ["/entrypoint.sh"]

# ──────────────────────────────────────────────────────────────────────────────
# Stage 2b: Ranger Usersync runtime
# ──────────────────────────────────────────────────────────────────────────────
FROM ubuntu:22.04 AS ranger-usersync

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV RANGER_USERSYNC_HOME=/opt/ranger/usersync

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        openjdk-17-jdk-headless python3 \
        curl wget netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ranger-builder /ranger-dist/ranger-*-usersync.tar.gz /tmp/ranger-usersync.tar.gz
RUN mkdir -p /opt/ranger/usersync && \
    tar -xzf /tmp/ranger-usersync.tar.gz -C /opt/ranger/usersync --strip-components=1 && \
    rm /tmp/ranger-usersync.tar.gz

COPY entrypoint-usersync.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]

# ──────────────────────────────────────────────────────────────────────────────
# Stage 2c: HDFS plugin (for embedding in Hadoop image)
# ──────────────────────────────────────────────────────────────────────────────
FROM ubuntu:22.04 AS ranger-hdfs-plugin

COPY --from=ranger-builder /ranger-dist/ranger-*-hdfs-plugin.tar.gz /tmp/ranger-hdfs-plugin.tar.gz
RUN mkdir -p /opt/ranger/hdfs-plugin && \
    tar -xzf /tmp/ranger-hdfs-plugin.tar.gz -C /opt/ranger/hdfs-plugin --strip-components=1 && \
    rm /tmp/ranger-hdfs-plugin.tar.gz

# ──────────────────────────────────────────────────────────────────────────────
# Stage 2d: Hive plugin (for embedding in Hive image)
# ──────────────────────────────────────────────────────────────────────────────
FROM ubuntu:22.04 AS ranger-hive-plugin

COPY --from=ranger-builder /ranger-dist/ranger-*-hive-plugin.tar.gz /tmp/ranger-hive-plugin.tar.gz
RUN mkdir -p /opt/ranger/hive-plugin && \
    tar -xzf /tmp/ranger-hive-plugin.tar.gz -C /opt/ranger/hive-plugin --strip-components=1 && \
    rm /tmp/ranger-hive-plugin.tar.gz
