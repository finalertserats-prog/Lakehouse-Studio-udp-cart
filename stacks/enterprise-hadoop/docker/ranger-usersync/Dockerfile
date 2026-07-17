# Apache Ranger Usersync 2.4.0
# Syncs Unix users from the container to Ranger Admin
FROM ubuntu:22.04

ARG RANGER_VERSION=2.8.0

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV RANGER_USERSYNC_HOME=/opt/ranger/usersync
ENV PATH=$PATH:${JAVA_HOME}/bin

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        openjdk-11-jdk-headless \
        python3 \
        curl wget \
        netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# Download Ranger Usersync
RUN wget -q \
        "https://archive.apache.org/dist/ranger/${RANGER_VERSION}/ranger-${RANGER_VERSION}-usersync.tar.gz" \
        -O /tmp/ranger-usersync.tar.gz && \
    mkdir -p /opt/ranger/usersync && \
    tar -xzf /tmp/ranger-usersync.tar.gz -C /opt/ranger/usersync --strip-components=1 && \
    rm /tmp/ranger-usersync.tar.gz

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
