# /// ci
# on = ["tag perf-*"]
# machine = "wohnzimmer"
# timeout_minutes = 60
# [env]
# WEIDA_PERF_SERVER = "10.0.0.8"
# ///
# The client side of the weida-perf LAN chain on wohnzimmer (crates/perf):
# `weida-perf serve` must already run on tuco-pc. Manual runs only
# (run_jobs); a `perf-*` tag also starts it.
. (Join-Path $env:CI_WORKSPACE '.zeughaus-ci\windows.ps1')
Run 'cargo' @('build', '--locked', '--release', '-p', 'weida-perf')
Run (Join-Path $env:CARGO_TARGET_DIR 'release\weida-perf.exe') @('client', '--server', $env:WEIDA_PERF_SERVER)
