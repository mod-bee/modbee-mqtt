# The modbee-mqtt image: `modbee serve`, the multi-core MQTT broker with the
# Kafka pipe. Published as ghcr.io/mod-bee/modbee-mqtt by
# .github/workflows/docker.yml; docs/docker.md is the manual.
#
#   docker build -t modbee -f docker/Dockerfile .     (from the repository root)
#   docker run -p 1883:1883 modbee                    the broker
#   docker run --rm modbee pub -h <host> -t x -m hi   the modbee command (docker/entrypoint.sh)
#
# The configuration is /etc/modbee/modbee.yaml - see docs/kafka.md; without
# one the broker runs with defaults (plain MQTT on 1883, one shard per core).
# /var/lib/modbee is where `retained: store: file` keeps retained messages;
# mount a volume there for them to outlive the container.
# Any value in the file can be set from the environment instead
# (MODBEE_BROKER_PORT, MODBEE_KAFKA_CLUSTERS_0_BOOTSTRAP_SERVERS, ...), and
# secrets read from files (password-file, or MODBEE_..._PASSWORD_FILE).
#
# `docker stop` sends SIGTERM: Kafka consumers commit, producers flush what is
# in flight (up to 5 s), and every client is told 0x8B Server shutting down -
# inside Docker's default 10-second grace period.

ARG DEBIAN=trixie-slim

FROM debian:${DEBIAN} AS build
ARG RDKAFKA_VERSION=2.15.1
RUN apt-get update && apt-get install -y --no-install-recommends \
        g++ make cmake pkg-config ca-certificates curl \
        libssl-dev libsasl2-dev libzstd-dev liblz4-dev zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*
RUN curl -fsSL -o /tmp/rdkafka.tgz \
        https://github.com/confluentinc/librdkafka/archive/refs/tags/v${RDKAFKA_VERSION}.tar.gz \
    && mkdir /tmp/rdkafka && tar xzf /tmp/rdkafka.tgz -C /tmp/rdkafka --strip-components=1 \
    && cd /tmp/rdkafka && ./configure --prefix=/usr/local --disable-curl >/dev/null \
    && make -j"$(nproc)" -C src >/dev/null && make -C src install >/dev/null && ldconfig
COPY . /src
RUN cmake -S /src -B /build -DCMAKE_BUILD_TYPE=Release -DMQTT_BUILD_CLI=ON -DMQTT_BUILD_SERVER=ON \
    && cmake --build /build --target modbee -j"$(nproc)" \
    && strip /build/cli/modbee

FROM debian:${DEBIAN}
LABEL org.opencontainers.image.title="modbee-mqtt" \
      org.opencontainers.image.description="MQTT 5.0/3.1.1 broker, one shard per core, with a pass-through Kafka pipe" \
      org.opencontainers.image.source="https://github.com/mod-bee/modbee-mqtt"
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates libssl3t64 libsasl2-2 libzstd1 liblz4-1 zlib1g \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --no-create-home --shell /usr/sbin/nologin modbee \
    && mkdir -p /etc/modbee && touch /etc/modbee/modbee.yaml \
    && mkdir -p /var/lib/modbee && chown modbee:modbee /var/lib/modbee
COPY --from=build /usr/local/lib/librdkafka.so.1 /usr/local/lib/
COPY --from=build /build/cli/modbee /usr/local/bin/modbee
COPY --chmod=755 docker/entrypoint.sh /usr/local/bin/modbee-entrypoint
RUN ldconfig && modbee check -C /etc/modbee/modbee.yaml -q
USER modbee
EXPOSE 1883 8883 9464
# A TCP check of the MQTT port: true once the listeners are up.
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s \
    CMD bash -c 'exec 3<>/dev/tcp/127.0.0.1/${MODBEE_BROKER_PORT:-1883}' || exit 1
STOPSIGNAL SIGTERM
ENTRYPOINT ["modbee-entrypoint"]
