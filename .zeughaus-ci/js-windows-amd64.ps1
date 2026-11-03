# /// ci
# on = ["push main", "tag v*", "nightly"]
# machine = "wohnzimmer"
# timeout_minutes = 60
# ///
# weida for JavaScript on wohnzimmer's host (docs/libraries/weida-js.md).
# libweida is built here, statically linked to the C runtime, in the warm
# workspace check-windows-amd64 uses. The runtimes are downloaded once into
# .ci-cache\js, at the js image's versions (build.sh, js_env). The rest
# mirrors js_check of build.sh, with build.ps1 and package.ps1; the job hands
# prebuilds/win32-x64/ on to publish-npm.
. (Join-Path $env:CI_WORKSPACE '.zeughaus-ci\windows.ps1')
# Windows PowerShell 5.1's progress bar slows Invoke-WebRequest down.
$ProgressPreference = 'SilentlyContinue'
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$tier = if ($env:CI_EVENT -eq 'nightly' -or $env:CI_EVENT -eq 'tag') { 'ci' } else { 'default' }

$nodeVersion = '26.13.0'
$bunVersion = '1.4.4'
$denoVersion = '2.9.8'
$js = Join-Path $cache 'js'
New-Item -ItemType Directory -Force -Path $js | Out-Null
Get-ChildItem -Path $js -Filter 'dl.*' -Directory | Remove-Item -Recurse -Force

# Downloads one tool into $Final through a temporary directory, so an
# interrupted download never leaves a half directory at the final path.
# $Fetch gets that directory and must leave the finished tool in its `out`.
function Install-Download([string]$Final, [string]$Probe, [scriptblock]$Fetch) {
    if (Test-Path -LiteralPath (Join-Path $Final $Probe)) {
        return
    }
    $tmp = Join-Path $js ('dl.' + [IO.Path]::GetRandomFileName())
    New-Item -ItemType Directory -Force -Path $tmp | Out-Null
    & $Fetch $tmp
    if (-not (Test-Path -LiteralPath (Join-Path $tmp "out\$Probe"))) {
        throw "the download for $Final holds no $Probe"
    }
    Move-Item -LiteralPath (Join-Path $tmp 'out') -Destination $Final
    Remove-Item -Recurse -Force -LiteralPath $tmp
}

$nodeDir = Join-Path $js "node-v$nodeVersion-win-x64"
Install-Download $nodeDir 'node.exe' {
    param($tmp)
    $zip = Join-Path $tmp 'download.zip'
    Invoke-WebRequest -UseBasicParsing -Uri "https://nodejs.org/dist/v$nodeVersion/node-v$nodeVersion-win-x64.zip" -OutFile $zip
    Expand-Archive -Path $zip -DestinationPath $tmp
    Rename-Item -LiteralPath (Join-Path $tmp "node-v$nodeVersion-win-x64") -NewName 'out'
}

$apiDir = Join-Path $js "node-v$nodeVersion-api"
Install-Download $apiDir 'node.lib' {
    param($tmp)
    $tgz = Join-Path $tmp 'headers.tar.gz'
    Invoke-WebRequest -UseBasicParsing -Uri "https://nodejs.org/dist/v$nodeVersion/node-v$nodeVersion-headers.tar.gz" -OutFile $tgz
    # By full path, so Git's tar never runs.
    Run "$env:SystemRoot\System32\tar.exe" @('-xzf', $tgz, '-C', $tmp)
    New-Item -ItemType Directory -Force -Path (Join-Path $tmp 'out') | Out-Null
    Move-Item -LiteralPath (Join-Path $tmp "node-v$nodeVersion\include") -Destination (Join-Path $tmp 'out\include')
    Invoke-WebRequest -UseBasicParsing -Uri "https://nodejs.org/dist/v$nodeVersion/win-x64/node.lib" -OutFile (Join-Path $tmp 'out\node.lib')
}

$bunDir = Join-Path $js "bun-$bunVersion"
Install-Download $bunDir 'bun.exe' {
    param($tmp)
    $zip = Join-Path $tmp 'download.zip'
    Invoke-WebRequest -UseBasicParsing -Uri "https://github.com/oven-sh/bun/releases/download/bun-v$bunVersion/bun-windows-x64.zip" -OutFile $zip
    Expand-Archive -Path $zip -DestinationPath $tmp
    Rename-Item -LiteralPath (Join-Path $tmp 'bun-windows-x64') -NewName 'out'
}

$denoDir = Join-Path $js "deno-$denoVersion"
Install-Download $denoDir 'deno.exe' {
    param($tmp)
    $zip = Join-Path $tmp 'download.zip'
    Invoke-WebRequest -UseBasicParsing -Uri "https://github.com/denoland/deno/releases/download/v$denoVersion/deno-x86_64-pc-windows-msvc.zip" -OutFile $zip
    Expand-Archive -Path $zip -DestinationPath (Join-Path $tmp 'out')
}

$env:PATH = "$nodeDir;$bunDir;$denoDir;$env:PATH"
Run 'node' @('--version')
Run 'bun' @('--version')
Run 'deno' @('--version')

# Only this job builds `release` in this workspace, so the check job's debug
# build keeps its cache.
$env:RUSTFLAGS = '-C target-feature=+crt-static'
Run 'cargo' @('build', '--release', '--locked', '-p', 'weida-c')
Remove-Item Env:RUSTFLAGS

$env:WEIDA_LIB_DIR = Join-Path $env:CARGO_TARGET_DIR 'release'
$env:NODE_API_DIR = $apiDir
Set-Location (Join-Path $env:CI_WORKSPACE 'crates\js\weida-js')
Run 'powershell' @('-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', '.\build.ps1')

if ($tier -eq 'ci') {
    $tests = 'test'
    $transports = @('inproc', 'quic')
} else {
    $tests = 'test/unit'
    $transports = @('inproc')
}
Run 'node' @('--test', $tests)
foreach ($t in $transports) {
    Run 'node' @('smoke.mjs', $t)
    Run 'bun' @('smoke.mjs', $t)
    Run 'deno' @('run', '--allow-read', '--allow-env', '--allow-ffi', 'smoke.mjs', $t)
}
if ($tier -eq 'ci') {
    Run 'powershell' @('-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', '.\package.ps1')
}

New-Item -ItemType Directory -Force -Path (Join-Path $env:CI_OUTPUT 'prebuilds') | Out-Null
Copy-Item -Recurse -Force -LiteralPath 'prebuilds\win32-x64' -Destination (Join-Path $env:CI_OUTPUT 'prebuilds')
