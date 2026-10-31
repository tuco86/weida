# Sourced by the platform check jobs, publish.sh, audit.sh and fuzz.sh; no
# header, so not a job. Run from the workspace root, with CI_PLATFORM set by
# the job's `platform`.

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
