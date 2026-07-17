# Apache Ranger Admin 2.4.0
# Production: techsophy.com uses Ranger 3.0.0-SNAPSHOT; closest stable release is 2.4.0
FROM ubuntu:22.04

ARG RANGER_VERSION=2.8.0

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV RANGER_HOME=/opt/ranger/admin
ENV PATH=$PATH:${JAVA_HOME}/bin

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        openjdk-11-jdk-headless \
        python3 python3-pip \
        curl wget \
        postgresql-client \
        netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# Download Ranger Admin
RUN wget -q \
        "https://archive.apache.org/dist/ranger/${RANGER_VERSION}/ranger-${RANGER_VERSION}-admin.tar.gz" \
        -O /tmp/ranger-admin.tar.gz && \
    mkdir -p /opt/ranger/admin && \
    tar -xzf /tmp/ranger-admin.tar.gz -C /opt/ranger/admin --strip-components=1 && \
    rm /tmp/ranger-admin.tar.gz

# Download PostgreSQL JDBC driver for Ranger DB
RUN wget -q \
        "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar" \
        -O /opt/ranger/admin/ews/lib/postgresql-42.7.3.jar

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 6080

ENTRYPOINT ["/entrypoint.sh"]
