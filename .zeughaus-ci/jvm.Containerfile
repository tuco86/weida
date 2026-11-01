# The image the jvm-linux-* jobs run in: the JDK the JNI glue over libweida
# and its Kotlin/Java API build against (docs/decisions/0046).
# - debian:trixie-slim rather than Temurin's Ubuntu base: the same glibc
#   libweida is built against.
# - build-essential: the C compiler the JNI glue and build.sh's
#   smoke_libweida need.
# - Temurin 25: the JDK, copied from its official image.
# The same file serves amd64 under podman and arm64 under Apple `container`.
FROM docker.io/library/debian:trixie-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY --from=docker.io/library/eclipse-temurin:25-jdk /opt/java/openjdk /opt/java/openjdk

ENV JAVA_HOME=/opt/java/openjdk PATH=/opt/java/openjdk/bin:$PATH
