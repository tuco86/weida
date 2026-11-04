# /// ci
# on = ["push main", "tag v*", "nightly"]
# machine = "wohnzimmer"
# timeout_minutes = 60
# ///
# weida's Python package on wohnzimmer's host (docs/libraries/weida-py.md).
# The wheel is built here with libweida statically linked to the C runtime, as
# js-windows-amd64 builds it, in the warm workspace check-windows-amd64 uses.
# uv, at python.Containerfile's version, is downloaded once into
# .ci-cache\uv-bin and installs CPython 3.13 and 3.14, free-threaded 3.14t and
# PyPy 3.11 into .ci-cache\uv-python. The rest mirrors python_check of
# build.sh. The job hands wheels\ on.
. (Join-Path $env:CI_WORKSPACE '.zeughaus-ci\windows.ps1')
$ProgressPreference = 'SilentlyContinue'
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$tier = if ($env:CI_EVENT -eq 'nightly' -or $env:CI_EVENT -eq 'tag') { 'ci' } else { 'default' }

$uvVersion = '0.12.2'
$tools = Join-Path $cache 'uv-bin'
New-Item -ItemType Directory -Force -Path $tools | Out-Null
Get-ChildItem -Path $tools -Filter 'dl.*' -Directory | Remove-Item -Recurse -Force
$uvDir = Join-Path $tools "uv-$uvVersion"
Install-Download $uvDir 'uv.exe' {
    param($tmp)
    $zip = Join-Path $tmp 'download.zip'
    Invoke-WebRequest -UseBasicParsing -Uri "https://github.com/astral-sh/uv/releases/download/$uvVersion/uv-x86_64-pc-windows-msvc.zip" -OutFile $zip
    Expand-Archive -Path $zip -DestinationPath (Join-Path $tmp 'out')
}
$env:PATH = "$uvDir;$env:PATH"
$env:UV_CACHE_DIR = Join-Path $cache 'uv'
$env:UV_PYTHON_INSTALL_DIR = Join-Path $cache 'uv-python'
$env:UV_PYTHON_PREFERENCE = 'only-managed'
Run 'uv' @('--version')

# hatch_build.py runs cargo build --release -p weida-c; with js-windows-amd64's
# RUSTFLAGS the two jobs share that release build.
$wheels = Join-Path $cache 'py-wheels'
if (Test-Path -LiteralPath $wheels) { Remove-Item -Recurse -Force -LiteralPath $wheels }
$env:RUSTFLAGS = '-C target-feature=+crt-static'
Run 'uv' @('build', '--wheel', '--python', 'cpython-3.14.7', '-o', $wheels, 'crates\py\weida-py')
Remove-Item Env:RUSTFLAGS
$found = @(Get-ChildItem -LiteralPath $wheels -Filter 'weida-*.whl')
if ($found.Count -ne 1) { throw "expected one wheel in $wheels, found $($found.Count)" }
if ($found[0].Name -notlike '*-py3-none-win_amd64.whl') { throw "the wheel is not py3-none-win_amd64: $($found[0].Name)" }
$wheel = $found[0].FullName
Run 'uvx' @('--from', 'twine==7.0.0', 'twine', 'check', $wheel)

# Single quotes only inside -c code: Windows PowerShell 5.1 does not escape
# a double quote it passes to a native command.
$pythons = @(
    @('cpython313', 'cpython-3.13.14'), @('cpython', 'cpython-3.14.7'),
    @('cpython-ft', 'cpython-3.14.7+freethreaded'), @('pypy', 'pypy-3.11.15')
)
$tests = if ($tier -eq 'ci') { 'crates\py\weida-py\tests' } else { 'crates\py\weida-py\tests\unit' }
foreach ($p in $pythons) {
    $name = $p[0]
    $venv = Join-Path $cache "venv-$name"
    $python = Join-Path $venv 'Scripts\python.exe'
    Run 'uv' @('venv', '--clear', '--python', $p[1], $venv)
    $packages = @($wheel, 'pytest', 'pytest-timeout', 'pytest-socket')
    if ($name -eq 'cpython' -or $name -eq 'cpython313') { $packages += 'cryptography' }
    Run 'uv' (@('pip', 'install', '--python', $venv) + $packages)
    Run $python @('-c', 'import sys, weida; print(sys.version); print(''libweida'', weida.version())')
    if ($name -eq 'cpython-ft') {
        Run $python @('-c', 'import sys, weida; assert not sys._is_gil_enabled(), ''importing weida re-enabled the GIL''; print(''free-threaded: GIL disabled'')')
    }
    Run $python @('-m', 'pytest', $tests)
}

if ($tier -eq 'ci') {
    $venv = Join-Path $cache 'venv-smoke'
    $python = Join-Path $venv 'Scripts\python.exe'
    Run 'uv' @('venv', '--clear', '--python', 'cpython-3.14.7', $venv)
    Run 'uv' @('pip', 'install', '--python', $venv, $wheel)
    $env:WEIDA_WORKSPACE_VERSION = & $python -c 'import tomllib; print(tomllib.load(open(''Cargo.toml'', ''rb''))[''workspace''][''package''][''version''])'
    if ($LASTEXITCODE -ne 0) { throw 'reading the workspace version failed' }
    $path = $env:PATH
    $env:PATH = "$env:SystemRoot\System32;$env:SystemRoot"
    Run $python @('crates\py\weida-py\smoke.py')
    $env:PATH = $path
}

$out = Join-Path $env:CI_OUTPUT 'wheels'
New-Item -ItemType Directory -Force -Path $out | Out-Null
Copy-Item -LiteralPath $wheel -Destination $out
