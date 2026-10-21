# /// ci
# on = ["push main", "tag v*", "cron 0 4 * * *"]
# machine = "wohnzimmer"
# timeout_minutes = 180
# ///
# Builds every test binary and example check-windows-amd64 runs, on the
# Windows runner's host, so the check starts warm. No artifact: the check runs
# in the same workspace.
. (Join-Path $env:CI_WORKSPACE '.zeughaus-ci\windows.ps1')

Run 'cargo' (@('test', '--locked', '--workspace') + $exclude + @('--no-run'))
