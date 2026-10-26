# THE ECLIPSE SPARKPLUG TCK, BUILT AND RUN HEADLESSLY.
#
# From modbee-iot-platform/docker/sparkplug/tck, with python3 added to the
# builder.
#
# The conformance kit is a HiveMQ extension: a Java project built with Gradle, dropped into a
# HiveMQ Community Edition broker's `extensions/` directory. Its own User Guide drives it from a
# Node web console in a browser, which is why the platform plan called containerising it "its own
# piece of work".
#
# IT DOES NOT NEED THE BROWSER. The console is only an MQTT client: the extension is driven
# entirely over control topics — `SPARKPLUG_TCK/TEST_CONTROL` to start a test,
# `SPARKPLUG_TCK/RESULT` for verdicts, `SPARKPLUG_TCK/LOG` for progress (read out of
# `tck/src/main/java/org/eclipse/sparkplug/tck/test/common/Constants.java`). So `driver.mjs` beside
# this file publishes and subscribes, and there is nothing to click.
#
# PINNED, like the rest of the rig. The TCK is built from the v3.0.0 tag rather than master, so a
# conformance result means something a year from now.

# ── stage 1: build the extension ────────────────────────────────────────────
FROM eclipse-temurin:11-jdk AS builder

# python3: the build's generateRequirements task runs a Python script, and the
# image no longer ships one.
RUN apt-get update && apt-get install -y --no-install-recommends git unzip curl ca-certificates python3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
ARG SPARKPLUG_REF=v3.0.0
RUN git clone --depth 1 --branch ${SPARKPLUG_REF} https://github.com/eclipse-sparkplug/sparkplug.git .

# GRADLE IS INSTALLED, NOT BOOTSTRAPPED FROM THE WRAPPER.
#
# `./gradlew` fails in a fresh clone with `ClassNotFoundException: GradleWrapperMain` — the
# repository does not ship `gradle/wrapper/gradle-wrapper.jar`, so there is nothing for the wrapper
# script to run. The VERSION still comes from the repository: `gradle-wrapper.properties` pins
# 7.6, which is also what Eclipse's own `DOCKER_BUILD/Dockerfile` installs, so this matches what
# the kit is built with upstream rather than whatever is current.
ARG GRADLE_VERSION=7.6
RUN curl -fsSL -o /tmp/gradle.zip "https://services.gradle.org/distributions/gradle-${GRADLE_VERSION}-bin.zip" \
    && unzip -q -d /opt /tmp/gradle.zip && rm /tmp/gradle.zip
ENV PATH="/opt/gradle-${GRADLE_VERSION}/bin:${PATH}"

WORKDIR /src/tck
# `hivemqExtensionZip` produces the extension. `--no-daemon` because a Gradle daemon outliving the
# build is a container layer that will not finish.
RUN gradle --no-daemon hivemqExtensionZip
# Unpacked here: the HiveMQ image has no unzip.
RUN mkdir /extension && cd /extension && unzip -q /src/tck/build/hivemq-extension/*.zip

# ── stage 2: HiveMQ CE with the extension installed ─────────────────────────
FROM hivemq/hivemq-ce:2024.3

USER root
COPY --from=builder /extension/ /opt/hivemq/extensions/

# A WEBSOCKET LISTENER, which the User Guide requires. Kept even though the driver uses plain TCP:
# the extension's own console support expects it to exist, and a config that differs from the
# documented one is a variable nobody needs in a conformance run.
COPY config.xml /opt/hivemq/conf/config.xml
# The image has no "hivemq" user: it runs as uid 10000 in group 0, which owns
# its files.
RUN chown -R 10000:0 /opt/hivemq/extensions /opt/hivemq/conf && chmod -R g+rwX /opt/hivemq/extensions /opt/hivemq/conf

USER 10000
