# The modbee-mqtt image: `modbee serve`, the multi-core MQTT broker with the
# Kafka pipe. Published as ghcr.io/mod-bee/modbee-mqtt by
# .github/workflows/docker.yml; docs/docker.md is the manual.
#
#   docker build -t modbee -f docker/Dockerfile .     (from the repository root)
#   docker run -p 1883:1883 modbee                    the broker
#   docker run --rm modbee pub -h <host> -t x -m hi   the modbee command (docker/entrypoint.sh)
#
# MQTT over TLS is served by wolfSSL, built here from a pinned release - the
# same engine a board runs, so the broker and a controller fail the same way
# (docs/security.md). OpenSSL is still in the image, but only because
# librdkafka uses it for TLS to Kafka. To build the broker on OpenSSL instead:
#
#   docker build --build-arg TLS_ENGINE=openssl -t modbee -f docker/Dockerfile .
#
# Which one an image has is its label io.modbee.tls-engine (`docker inspect`).
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
ARG TLS_ENGINE=wolfssl
# A release tag, and the SHA-256 of GitHub's tarball for it. Change both
# together: a mismatch stops the build rather than building something unknown.
ARG WOLFSSL_VERSION=5.9.4
ARG WOLFSSL_SHA256=7256bfc89b183a75183806c7debfa203443873b0b4a562e1b80d68e01b45ac57
RUN case "${TLS_ENGINE}" in wolfssl|openssl) ;; \
        *) echo "TLS_ENGINE must be wolfssl or openssl, not '${TLS_ENGINE}'" >&2; exit 1 ;; esac
# autoconf, automake and libtool are for wolfSSL: a tag's tarball carries no
# generated configure script.
# gRPC and protobuf are the Application API's (docs/app-api.md): Debian's own.
RUN apt-get update && apt-get install -y --no-install-recommends \
        g++ make cmake pkg-config ca-certificates curl autoconf automake libtool \
        libssl-dev libsasl2-dev libzstd-dev liblz4-dev zlib1g-dev \
        libgrpc++-dev libprotobuf-dev protobuf-compiler protobuf-compiler-grpc \
    && rm -rf /var/lib/apt/lists/*
RUN curl -fsSL -o /tmp/rdkafka.tgz \
        https://github.com/confluentinc/librdkafka/archive/refs/tags/v${RDKAFKA_VERSION}.tar.gz \
    && mkdir /tmp/rdkafka && tar xzf /tmp/rdkafka.tgz -C /tmp/rdkafka --strip-components=1 \
    && cd /tmp/rdkafka && ./configure --prefix=/usr/local --disable-curl >/dev/null \
    && make -j"$(nproc)" -C src >/dev/null && make -C src install >/dev/null && ldconfig
# wolfSSL with exactly what the library asks of it, and the reason for each:
#   opensslall, opensslextra   the OpenSSL-named API the transports, the
#                              listener and the identity are written against -
#                              X509_STORE, SSL_CTX_set_verify, PEM and BIO
#                              memory buffers, EVP_PKEY, X509_check_host; all
#                              also turns on subject alternative names
#   certgen, certext, keygen   MqttIdentity mints its own certificate with
#                              wolfCrypt: an EC P-256 key, then CA:TRUE, key
#                              usage, extended key usage and a subject key id
#   tls13, session-ticket      TLS 1.3 and resumption, as with OpenSSL
#   sni                        the client names the broker it is dialling
#   curve25519                 the key exchange today's clients send first
#                              (X25519, and X25519MLKEM768 with it). Without it
#                              a TLS 1.3 client is sent back to try again and
#                              the broker settles on P-521: an extra round trip
#                              and a costlier curve on every handshake -
#                              measured, a third fewer handshakes a second.
#   sp, sp-asm, intelasm or    the assembly paths, for the ECC a handshake and
#   armasm                     the AES-GCM every record spends its time in.
#                              Each checks the CPU at run time for the AES,
#                              SHA and wide-multiply instructions it would use,
#                              so the image still runs on a CPU without them
#                              (a Raspberry Pi 4 has no AES instructions).
# Configured through autotools rather than wolfSSL's CMake because only
# autotools offers the assembly options; it installs the CMake package all the
# same (lib/cmake/wolfssl), which is what find_package(wolfssl) reads.
RUN if [ "${TLS_ENGINE}" = wolfssl ]; then \
        case "$(uname -m)" in \
            x86_64) asm=--enable-intelasm ;; \
            aarch64) asm=--enable-armasm ;; \
            *) asm= ;; \
        esac \
        && curl -fsSL -o /tmp/wolfssl.tgz \
            https://github.com/wolfSSL/wolfssl/archive/refs/tags/v${WOLFSSL_VERSION}-stable.tar.gz \
        && echo "${WOLFSSL_SHA256}  /tmp/wolfssl.tgz" | sha256sum -c - \
        && mkdir /tmp/wolfssl && tar xzf /tmp/wolfssl.tgz -C /tmp/wolfssl --strip-components=1 \
        && cd /tmp/wolfssl && ./autogen.sh >/dev/null \
        && ./configure --prefix=/usr/local --disable-static \
            --enable-opensslall --enable-opensslextra \
            --enable-certgen --enable-certext --enable-keygen \
            --enable-tls13 --enable-session-ticket --enable-sni --enable-curve25519 \
            --enable-sp --enable-sp-asm $asm \
            --disable-examples --disable-crypttests --disable-benchmark >/dev/null \
        && make -j"$(nproc)" >/dev/null && make install >/dev/null && ldconfig \
        && rm -rf /tmp/wolfssl /tmp/wolfssl.tgz; \
    fi
COPY . /src
# The engine is checked, not trusted: CMake's own line naming it has to say
# the one that was asked for, or the build stops. Without the check a wolfSSL
# that failed to install would quietly give an OpenSSL broker. For OpenSSL,
# find_package(wolfssl) is switched off outright.
#
# /out/lib collects every library the binary loads from /usr/local -
# librdkafka, and libwolfssl when it is the engine - so the image below copies
# one directory whichever engine was built.
RUN if [ "${TLS_ENGINE}" = wolfssl ]; then \
        engine=wolfSSL find=-Dwolfssl_DIR=/usr/local/lib/cmake/wolfssl; \
    else \
        engine=OpenSSL find=-DCMAKE_DISABLE_FIND_PACKAGE_wolfssl=ON; \
    fi \
    && cmake -S /src -B /build -DCMAKE_BUILD_TYPE=Release -DMQTT_BUILD_CLI=ON -DMQTT_BUILD_SERVER=ON \
        "$find" > /tmp/configure.log \
    && grep "modbee-mqtt: .* TLS engine\|modbee: librdkafka\|modbee: gRPC" /tmp/configure.log \
    && grep -q "modbee-mqtt: ${engine} found - it is the TLS engine" /tmp/configure.log \
    && grep -q "modbee: librdkafka .* found" /tmp/configure.log \
    && grep -q "modbee: gRPC .* found - the Application API is built" /tmp/configure.log \
    && cmake --build /build --target modbee -j"$(nproc)" \
    && strip /build/cli/modbee \
    && mkdir -p /out/lib \
    && ldd /build/cli/modbee | awk '$3 ~ /^\/usr\/local\/lib\// { print $3 }' \
        | xargs -r cp -L -t /out/lib/ \
    && strip --strip-unneeded /out/lib/* \
    && ls -l /out/lib \
    && ldd /build/cli/modbee | awk '$3 ~ /^\/(usr\/)?lib\// { print $3 }' | xargs -r readlink -f \
        | xargs -r dpkg -S | cut -d: -f1 | sort -u > /out/packages \
    && test -s /out/packages \
    && echo "runtime packages:" $(cat /out/packages)

FROM debian:${DEBIAN}
ARG TLS_ENGINE=wolfssl
LABEL org.opencontainers.image.title="modbee-mqtt" \
      org.opencontainers.image.description="MQTT 5.0/3.1.1 broker, one shard per core, with a pass-through Kafka pipe" \
      org.opencontainers.image.source="https://github.com/mod-bee/modbee-mqtt" \
      io.modbee.tls-engine="${TLS_ENGINE}"
# Exactly the packages the binary loads from Debian - gRPC's and protobuf's
# with the rest, listed by the build stage from the binary itself rather than
# named here by hand. libssl3t64 is librdkafka's (TLS to Kafka), whichever
# engine serves MQTT.
COPY --from=build /out/packages /tmp/packages
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates libssl3t64 libsasl2-2 libzstd1 liblz4-1 zlib1g $(cat /tmp/packages) \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --no-create-home --shell /usr/sbin/nologin modbee \
    && mkdir -p /etc/modbee && touch /etc/modbee/modbee.yaml \
    && mkdir -p /var/lib/modbee && chown modbee:modbee /var/lib/modbee
COPY --from=build /out/lib/ /usr/local/lib/
COPY --from=build /build/cli/modbee /usr/local/bin/modbee
COPY --chmod=755 docker/entrypoint.sh /usr/local/bin/modbee-entrypoint
RUN ldconfig && modbee check -C /etc/modbee/modbee.yaml -q
USER modbee
EXPOSE 1883 8883 9464 7660
# A TCP check of the MQTT ports: healthy once EITHER listener accepts a
# connection - the plain one or the TLS one - so a broker with plain MQTT
# turned off (broker: plain: false) is not reported unhealthy. Ports moved
# with MODBEE_BROKER_PORT / MODBEE_BROKER_TLS_PORT are followed; ports moved
# only in modbee.yaml are not. bash's /dev/tcp, so the image needs nothing
# extra installed.
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s \
    CMD bash -c 'for port in ${MODBEE_BROKER_PORT:-1883} ${MODBEE_BROKER_TLS_PORT:-8883}; do (exec 3<>/dev/tcp/127.0.0.1/$port) 2>/dev/null && exit 0; done; exit 1'
STOPSIGNAL SIGTERM
ENTRYPOINT ["modbee-entrypoint"]
