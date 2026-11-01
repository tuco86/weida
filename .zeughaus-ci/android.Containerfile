# The image the android-libweida job runs in: libweida cross-built for the
# four Android ABIs with the NDK (docs/decisions/0046). linux-amd64 only,
# because Google ships the NDK for linux-x86_64 hosts only.
# - rust:1.99.1-trixie: equal to trixie.Containerfile's base; change both
#   together, as its comment says for the toolchain.
# - unzip: unpacks the command-line tools.
# - Temurin 25: sdkmanager is a Java program; the same JDK jvm.Containerfile
#   carries.
# - the NDK and one platform, through sdkmanager; the Rust targets of the
#   four ABIs; cargo-ndk, which points cargo at the NDK's linkers.
FROM docker.io/library/rust:1.99.1-trixie

RUN apt-get update \
    && apt-get install -y --no-install-recommends unzip \
    && rm -rf /var/lib/apt/lists/*

COPY --from=docker.io/library/eclipse-temurin:25-jdk /opt/java/openjdk /opt/java/openjdk

ENV JAVA_HOME=/opt/java/openjdk PATH=/opt/java/openjdk/bin:$PATH

# Command-line tools 23.0, build 16111833: `cmdline-tools;latest` in
# https://dl.google.com/android/repository/repository2-3.xml on 2026-11-01.
RUN curl -fsSLo /tmp/cmdline-tools.zip \
        https://dl.google.com/android/repository/commandlinetools-linux-16111833_latest.zip \
    && mkdir -p /opt/android-sdk/cmdline-tools \
    && unzip -q /tmp/cmdline-tools.zip -d /opt/android-sdk/cmdline-tools \
    && mv /opt/android-sdk/cmdline-tools/cmdline-tools /opt/android-sdk/cmdline-tools/latest \
    && rm /tmp/cmdline-tools.zip

ENV ANDROID_HOME=/opt/android-sdk PATH=/opt/android-sdk/cmdline-tools/latest/bin:$PATH

# NDK 30.0.16248370: the newest stable `ndk;` entry, platform android-37.2
# the newest stable `platforms;android-` entry, both of `sdkmanager --list`
# (repository2-3.xml) on 2026-11-01.
RUN yes | sdkmanager --licenses >/dev/null \
    && sdkmanager "ndk;30.0.16248370" "platforms;android-37.2"

ENV ANDROID_NDK_HOME=/opt/android-sdk/ndk/30.0.16248370

RUN rustup target add aarch64-linux-android armv7-linux-androideabi x86_64-linux-android i686-linux-android

# cargo-ndk 4.1.2: the newest version of `cargo search cargo-ndk` on
# 2026-11-01.
RUN cargo install --locked cargo-ndk --version 4.1.2
