# Dot-sourced by the Windows jobs; no header, so not a job.
# Sets `Run`, `Install-Nextest`, `Install-Download`, `$cache`, `$exclude` and
# CARGO_TARGET_DIR.
$ErrorActionPreference = 'Stop'

# Runs a native command and ends the script with its exit code when it fails.
# Its output goes straight to the job's console, not through a pipe: so cargo
# and nextest see a terminal and color their output, and their UTF-8 is not
# decoded again with the console codepage. 'Continue' because Windows
# PowerShell 5.1 turns a native command's stderr into errors under 'Stop'
# where it captures it.
function Run([string]$Command, [string[]]$Arguments = @()) {
    $previous = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    $clock = [Diagnostics.Stopwatch]::StartNew()
    try {
        & $Command @Arguments
        $code = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $previous
    }
    $seconds = [int][Math]::Round($clock.Elapsed.TotalSeconds)
    if ($code -ne 0) {
        Write-Host "== $Command $($Arguments -join ' ') failed after $seconds s"
        Write-Host "$Command $($Arguments -join ' ') failed with exit code $code"
        exit $code
    }
    Write-Host "== $Command $($Arguments -join ' '): $seconds s"
}

# The scheduled nightly round (CI_ACTOR `nightly`) starts the workspace from
# nothing, once a day, as build.sh does; a nightly queued on demand stays
# warm (channels.toml).
if ($env:CI_EVENT -eq 'nightly' -and $env:CI_ACTOR -eq 'nightly') {
    $marker = Join-Path $env:CI_WORKSPACE ".ci-cache\cold-$env:CI_PIPELINE"
    if (-not (Test-Path $marker)) {
        Write-Host '== cold start: wiping the workspace for the scheduled nightly'
        Run 'git' @('-C', $env:CI_WORKSPACE, 'clean', '-ffdxq')
        New-Item -ItemType Directory -Force -Path (Join-Path $env:CI_WORKSPACE '.ci-cache') | Out-Null
        New-Item -ItemType File -Force -Path $marker | Out-Null
    }
}

$cache = Join-Path $env:CI_WORKSPACE '.ci-cache'
$env:CARGO_TARGET_DIR = Join-Path $cache 'target'

# cargo-nextest from its prebuilt release into `$cache\bin`, kept between runs
# by the warm workspace; on PATH so `cargo nextest` finds it.
function Install-Nextest {
    $bin = Join-Path $cache 'bin'
    if (-not (Test-Path (Join-Path $bin 'cargo-nextest.exe'))) {
        New-Item -ItemType Directory -Force -Path $bin | Out-Null
        $zip = Join-Path $env:TEMP "cargo-nextest-$PID.zip"
        [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
        Invoke-WebRequest -UseBasicParsing -Uri 'https://get.nexte.st/0.9/windows' -OutFile $zip
        Expand-Archive -Path $zip -DestinationPath $bin -Force
        Remove-Item -LiteralPath $zip
    }
    $env:PATH = "$bin;$env:PATH"
}

# Downloads one tool into $Final through a temporary directory, so an
# interrupted download never leaves a half directory at the final path.
# $Fetch gets that directory and must leave the finished tool in its `out`.
# Callers remove stale `dl.*` directories of their tool directory first.
function Install-Download([string]$Final, [string]$Probe, [scriptblock]$Fetch) {
    if (Test-Path -LiteralPath (Join-Path $Final $Probe)) {
        return
    }
    $tmp = Join-Path (Split-Path -Parent $Final) ('dl.' + [IO.Path]::GetRandomFileName())
    New-Item -ItemType Directory -Force -Path $tmp | Out-Null
    & $Fetch $tmp
    if (-not (Test-Path -LiteralPath (Join-Path $tmp "out\$Probe"))) {
        throw "the download for $Final holds no $Probe"
    }
    Move-Item -LiteralPath (Join-Path $tmp 'out') -Destination $Final
    Remove-Item -Recurse -Force -LiteralPath $tmp
}

# wohnzimmer has no Python; the bindings' test binaries link libpython.
$exclude = @(
    '--exclude', 'weida-py-core', '--exclude', 'weida-zmq-py',
    '--exclude', 'weida-nng-py', '--exclude', 'weida-mqtt-py', '--exclude', 'weida-amqp-py',
    '--exclude', 'weida-nats-py'
)
