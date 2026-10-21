# /// ci
# needs = ["build-windows-amd64"]
# machine = "wohnzimmer"
# timeout_minutes = 180
# ///
# clippy, the tests and the doctests on the Windows runner's host, in the
# workspace the build left warm: the unit tier on a push or manual run, both
# tiers on the nightly cron and a release tag. `check_platform` of build.sh.
. (Join-Path $env:CI_WORKSPACE '.zeughaus-ci\windows.ps1')

# The feature rows of docs/LOOP.md §6: crate, feature.
$features = @(
    @('weida', 'blocking'), @('weida', 'swarm'), @('weida-zmq', 'blocking'),
    @('weida-mqtt', 'blocking'), @('weida-nng', 'blocking')
)
$tier = if ($env:CI_EVENT -eq 'cron' -or $env:CI_EVENT -eq 'tag') { 'ci' } else { 'default' }

Run 'cargo' (@('clippy', '--locked', '--workspace') + $exclude + @('--all-targets', '--', '-D', 'warnings'))
Run 'cargo' @('clippy', '--locked', '-p', 'weida', '--no-default-features', '--all-targets', '--', '-D', 'warnings')
Install-Nextest
Run 'cargo' (@('nextest', 'run', '--locked', '--workspace') + $exclude + @('--profile', $tier, '--no-fail-fast'))
foreach ($row in $features) {
    Run 'cargo' @('clippy', '--locked', '-p', $row[0], '--features', $row[1], '--all-targets', '--', '-D', 'warnings')
    Run 'cargo' @('nextest', 'run', '--locked', '-p', $row[0], '--features', $row[1], '--profile', $tier, '--no-fail-fast')
}
Run 'cargo' (@('test', '--locked', '--workspace') + $exclude + @('--doc'))
