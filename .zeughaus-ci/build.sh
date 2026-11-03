# Sourced by the platform check jobs, the c-abi jobs, the language jobs
# (python-, js-, jvm-, beam-, go-linux-*, js-darwin-arm64, swift-darwin-arm64,
# android-libweida), publish.sh, publish-npm.sh, audit.sh and fuzz.sh; no
# header, so not a job.
# Run from the workspace root, with CI_PLATFORM set by the job's `platform`.

# The scheduled nightly round, as opposed to a nightly that `ci_run` or a
# rerun queued on demand (CI_ACTOR is then `<user>@<host>` or `rerun of #n`).
scheduled_nightly() {
    [ "$CI_EVENT" = nightly ] && [ "${CI_ACTOR:-}" = nightly ]
}

# The scheduled nightly starts every place's workspace from nothing, once a
# day: its first job at a place removes everything git does not track
# (caches, target directories, virtualenvs, package builds) and leaves a
# marker, so its later jobs there build on what the first one left. A
# nightly queued on demand runs warm (channels.toml).
if scheduled_nightly && [ ! -e "$CI_WORKSPACE/.ci-cache/cold-$CI_PIPELINE" ]; then
    printf '\n== cold start: wiping the workspace for the scheduled nightly\n'
    git -c safe.directory='*' -C "$CI_WORKSPACE" clean -ffdxq
    mkdir -p "$CI_WORKSPACE/.ci-cache"
    : >"$CI_WORKSPACE/.ci-cache/cold-$CI_PIPELINE"
fi

# Cargo's home and target directory live in the workspace, which the jobs of
# one place share and keep warm.
cargo_env() {
    cache="$CI_WORKSPACE/.ci-cache"
    export CARGO_HOME="$cache/cargo"
    export CARGO_TARGET_DIR="$cache/target"
    export CARGO_TERM_COLOR=always
}

# Runs one command as a visible, timed step. Its failure returns the
# command's code, so under `set -e` the job stops there; the `if` only
# switches `set -e` off for the command's own body.
step() {
    printf '\n== %s\n' "$*"
    step_start=$(date +%s)
    if "$@"; then
        printf '== %s: %s s\n' "$*" "$(( $(date +%s) - step_start ))"
    else
        step_code=$?
        printf '== %s failed after %s s\n' "$*" "$(( $(date +%s) - step_start ))"
        return "$step_code"
    fi
}

# cargo-nextest from its prebuilt release into the workspace's cargo home,
# where cargo finds subcommands; kept between runs by the warm workspace.
nextest_env() {
    [ -x "$CARGO_HOME/bin/cargo-nextest" ] && return
    case "$CI_PLATFORM" in
        linux-amd64) asset=linux ;;
        linux-arm64) asset=linux-arm ;;
        darwin-arm64) asset=mac ;;
        *) echo "no cargo-nextest for $CI_PLATFORM" >&2; return 1 ;;
    esac
    mkdir -p "$CARGO_HOME/bin"
    curl -LsSf "https://get.nexte.st/0.9/$asset" | tar zxf - -C "$CARGO_HOME/bin"
}

# The feature rows of docs/LOOP.md §6: crate:feature.
features="weida:swarm weida-zmq:blocking weida-mqtt:blocking weida-nng:blocking weida-http:h3"

# The nextest profile (`.config/nextest.toml`): a push or manual run takes the
# unit tier; the nightly and a release tag take both tiers.
tier() {
    case "$CI_EVENT" in
        nightly | tag) echo ci ;;
        *) echo default ;;
    esac
}

# clippy, the tests and the doctests, which nextest does not run: what every
# platform checks. `$exclude` is empty except where a job sets it.
check_platform() {
    t=$(tier)
    step cargo clippy --locked --workspace $exclude --all-targets -- -D warnings
    step cargo clippy --locked -p weida --no-default-features --all-targets -- -D warnings
    nextest_env
    step cargo nextest run --locked --workspace $exclude --profile "$t" --no-fail-fast
    for row in $features; do
        crate=${row%%:*} feature=${row#*:}
        step cargo clippy --locked -p "$crate" --features "$feature" --all-targets -- -D warnings
        step cargo nextest run --locked -p "$crate" --features "$feature" --profile "$t" --no-fail-fast
    done
    step cargo test --locked --workspace $exclude --doc
}

# The doc step of docs/LOOP.md §6, with warnings as errors.
check_docs() {
    step cargo clean --doc
    export RUSTDOCFLAGS="-D warnings"
    step cargo doc --locked --workspace --no-deps
    for c in weida weida-mqtt weida-amqp weida-nats weida-nng weida-http; do
        step cargo doc --locked -q -p "$c" --no-default-features --no-deps
    done
    for c in weida weida-zmq weida-mqtt weida-nng weida-broker weida-http; do
        step cargo doc --locked -q -p "$c" --all-features --no-deps
    done
    unset RUSTDOCFLAGS
}

# The C ABI and the Python package on it (docs/decisions/0045): the committed
# headers match what cbindgen derives from crates/weida-c, the Python tests
# run against an editable install, and libweida, weida.h and the wheel are
# handed on in $CI_OUTPUT for the language jobs (lib/, lib-static/, include/,
# wheels/): the python- jobs install the wheel, the js-, jvm- and beam- jobs
# build their native glue against lib/, the go- jobs link lib-static/'s
# libweida.a. The nightly and a release tag also run the C smoke program
# and the wheel's smoke in a fresh virtualenv.
c_abi() {
    step crates/weida-c/header.sh --verify

    venv="$CI_WORKSPACE/.ci-cache/venv"
    step python3 -m venv "$venv"
    step "$venv/bin/pip" install cffi hatchling pytest pytest-timeout pytest-socket cryptography
    step "$venv/bin/pip" install --no-build-isolation -e crates/py/weida-py

    case $(tier) in
        ci) step "$venv/bin/python" -m pytest crates/py/weida-py/tests ;;
        *) step "$venv/bin/python" -m pytest crates/py/weida-py/tests/unit ;;
    esac

    # The wheel build runs `cargo build --release -p weida-c` (hatch_build.py),
    # so libweida.so exists afterwards. The static library is not one of the
    # manifest's crate types (crates/weida-c/Cargo.toml) and is built here; it
    # gets a directory of its own, so that a -L to it cannot pick the shared one.
    mkdir -p "$CI_OUTPUT/lib" "$CI_OUTPUT/lib-static" "$CI_OUTPUT/include" "$CI_OUTPUT/wheels"
    step "$venv/bin/pip" wheel --no-build-isolation --no-deps -w "$CI_OUTPUT/wheels" crates/py/weida-py
    step cargo rustc --release --locked -p weida-c --lib --crate-type staticlib
    cp "$CARGO_TARGET_DIR/release/libweida.so" "$CI_OUTPUT/lib/"
    cp "$CARGO_TARGET_DIR/release/libweida.a" "$CI_OUTPUT/lib-static/"
    cp crates/weida-c/include/weida.h "$CI_OUTPUT/include/"

    if [ "$(tier)" = ci ]; then
        step crates/weida-c/smoke.sh
        WEIDA_PY_PYTHON="$venv/bin/python" step crates/py/weida-py/package.sh
    fi
}

# The libweida a c-abi job of this platform handed on (c_abi's layout).
libweida_input() {
    echo "$CI_INPUTS/c-abi-$CI_PLATFORM"
}

# Compiles crates/weida-c/tests/c/smoke.c with this image's C compiler
# against the handed-on header and library and runs it: proves the C
# toolchain every binding's glue needs, and that libweida, built on trixie,
# loads with this image's libc. Prints "c smoke: ok".
smoke_libweida() {
    in=$(libweida_input)
    step cc -std=c11 -Wall -Wextra -Werror -I "$in/include" \
        crates/weida-c/tests/c/smoke.c -L "$in/lib" -lweida -o /tmp/weida-c-smoke
    step env LD_LIBRARY_PATH="$in/lib" /tmp/weida-c-smoke
}

# Node, Bun and Deno for a host job, at the js image's versions: the image
# node:26-trixie ran Node 26.13.0 in pipeline #214, and js.Containerfile pins
# Bun 1.4.4 and Deno 2.9.8. These versions must follow js.Containerfile.
# Each tool is downloaded once into the warm workspace, through a temporary
# directory, so an interrupted download never leaves a half directory at
# the final path.
js_env() {
    node_version=26.13.0 bun_version=1.4.4 deno_version=2.9.8
    case "$CI_PLATFORM" in
        darwin-arm64) node_asset=darwin-arm64 bun_asset=bun-darwin-aarch64 deno_asset=deno-aarch64-apple-darwin ;;
        *) echo "no JavaScript runtimes for $CI_PLATFORM" >&2; return 1 ;;
    esac
    js="$CI_WORKSPACE/.ci-cache/js"
    node_dir="$js/node-v$node_version-$node_asset"
    bun_dir="$js/bun-$bun_version"
    deno_dir="$js/deno-$deno_version"
    mkdir -p "$js"
    rm -rf "$js"/dl.*

    if [ ! -x "$node_dir/bin/node" ]; then
        tmp=$(mktemp -d "$js/dl.XXXXXX")
        curl -fLsS "https://nodejs.org/dist/v$node_version/node-v$node_version-$node_asset.tar.gz" |
            tar xzf - -C "$tmp"
        [ -x "$tmp/node-v$node_version-$node_asset/bin/node" ]
        mv "$tmp/node-v$node_version-$node_asset" "$node_dir"
        rm -rf "$tmp"
    fi
    if [ ! -x "$bun_dir/bun" ]; then
        js_fetch_zip "https://github.com/oven-sh/bun/releases/download/bun-v$bun_version/$bun_asset.zip" bun "$bun_dir"
    fi
    if [ ! -x "$deno_dir/deno" ]; then
        js_fetch_zip "https://github.com/denoland/deno/releases/download/v$deno_version/$deno_asset.zip" deno "$deno_dir"
    fi
    export PATH="$node_dir/bin:$bun_dir:$deno_dir:$PATH"
}

# js_env's download of one zip holding the executable <binary> into <dir>.
js_fetch_zip() {
    tmp=$(mktemp -d "$js/dl.XXXXXX")
    curl -fLsS -o "$tmp/download.zip" "$1"
    unzip -oqj "$tmp/download.zip" -d "$tmp/out"
    [ -x "$tmp/out/$2" ]
    mv "$tmp/out" "$3"
    rm -rf "$tmp"
}

# The JavaScript package's check (docs/libraries/weida-js.md) against the
# libweida in the directory $1: the Node-API addon built against it, the unit
# tests under Node and the in-process smoke under Node, Bun and Deno; the
# nightly and a release tag also run the integration tests, the QUIC smoke
# and package.sh. The addon and its libweida are handed on as
# $CI_OUTPUT/prebuilds/<target>/, which publish-npm reads as
# $CI_INPUTS/<job>/prebuilds/<target>/.
js_check() {
    step node --version
    step bun --version
    step deno --version
    step cc --version

    cd crates/js/weida-js
    step env WEIDA_LIB_DIR="$1" ./build.sh
    case $(tier) in
        ci) tests=test transports="inproc quic" ;;
        *) tests=test/unit transports=inproc ;;
    esac
    step node --test "$tests"
    for transport in $transports; do
        step node smoke.mjs "$transport"
        step bun smoke.mjs "$transport"
        step deno run --allow-read --allow-env --allow-ffi smoke.mjs "$transport"
    done
    if [ "$(tier)" = ci ]; then
        step ./package.sh
    fi

    target=$(node -p 'process.platform + "-" + process.arch')
    mkdir -p "$CI_OUTPUT/prebuilds"
    cp -R "prebuilds/$target" "$CI_OUTPUT/prebuilds/"
}
