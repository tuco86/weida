# The image the jvm-linux-* jobs run in: the JDK the JNI glue over libweida
# and its Java API build against (docs/decisions/0046,
# docs/libraries/weida-jvm.md).
# - debian:trixie-slim rather than Temurin's Ubuntu base: the same glibc
#   libweida is built against.
# - build-essential: the C compiler the JNI glue and build.sh's
#   smoke_libweida need; curl downloads the JUnit jar below; git wipes the
#   workspace on the scheduled nightly (build.sh).
# - Temurin 25: the JDK, copied from its official image.
# - the JUnit Platform Console Standalone jar, which runs the tests of
#   crates/jvm/weida-jvm (test.sh).
# The same file serves amd64 under podman and arm64 under Apple `container`.
FROM docker.io/library/debian:trixie-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates build-essential curl git \
    && rm -rf /var/lib/apt/lists/*

COPY --from=docker.io/library/eclipse-temurin:25-jdk /opt/java/openjdk /opt/java/openjdk

ENV JAVA_HOME=/opt/java/openjdk PATH=/opt/java/openjdk/bin:$PATH

# junit-platform-console-standalone 6.1.3: the newest release of
# maven-metadata.xml on Maven Central on 2026-11-02 (published 2026-08-07);
# its sha256 is the one Maven Central serves beside it. Java 17 and up.
# crates/jvm/weida-jvm/develop.sh pins the same version and sha256.
RUN mkdir -p /opt/junit \
    && curl -fsSLo /opt/junit/junit-platform-console-standalone.jar \
        https://repo1.maven.org/maven2/org/junit/platform/junit-platform-console-standalone/6.1.3/junit-platform-console-standalone-6.1.3.jar \
    && echo "e62b96ac475dbcde8599ea905d088f65d90778f86e259b856a49fa5c4ea256ec  /opt/junit/junit-platform-console-standalone.jar" \
        | sha256sum -c -

ENV JUNIT_CONSOLE_JAR=/opt/junit/junit-platform-console-standalone.jar
