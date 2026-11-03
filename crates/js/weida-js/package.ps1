# Packs the package and proves the tarball works with no Rust toolchain
# present: the workspace version goes into the packed package.json, the
# tarball installs offline into a fresh empty project, and smoke.mjs runs
# there over both transports under node, bun and deno, with PATH reduced to
# those three runtimes plus %SystemRoot%\System32 and %SystemRoot%. The
# Windows twin of package.sh.
#
# Needs prebuilds\<target>\weida.node (build.ps1) first; packs every prebuild
# present. The tarball stays in $env:CARGO_TARGET_DIR\npm.
$ErrorActionPreference = 'Stop'

# Runs a native command and throws when it fails. 'Continue' because Windows
# PowerShell 5.1 turns a native command's stderr into errors under 'Stop'.
function Invoke-Native([string]$Command, [string[]]$Arguments) {
    $previous = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        & $Command @Arguments
        $code = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $previous
    }
    if ($code -ne 0) {
        throw "$Command failed with exit code $code"
    }
}

$dir = $PSScriptRoot
$root = Split-Path -Parent (Split-Path -Parent (Split-Path -Parent $dir))

$targetDir = if ($env:CARGO_TARGET_DIR) { $env:CARGO_TARGET_DIR } else { Join-Path $root 'target' }
$out = Join-Path $targetDir 'npm'
New-Item -ItemType Directory -Force -Path $out | Out-Null

if (-not (Get-ChildItem -Path (Join-Path $dir 'prebuilds\*\weida.node') -ErrorAction SilentlyContinue)) {
    throw 'package.ps1: prebuilds/*/weida.node is missing; run build.ps1 first'
}

# [workspace.package] version of the root Cargo.toml, already valid semver.
$cargoToml = Join-Path $root 'Cargo.toml'
$version = $null
$section = $null
foreach ($line in Get-Content -LiteralPath $cargoToml) {
    if ($line -match '^\[') {
        $section = $line.Trim()
    } elseif ($section -eq '[workspace.package]' -and $line -match '^version\s*=\s*"([^"]+)"') {
        $version = $Matches[1]
        break
    }
}
if (-not $version) {
    throw "package.ps1: no [workspace.package] version in $cargoToml"
}

$fresh = Join-Path $env:TEMP "weida-js-package-$PID"
$oldPath = $env:PATH
try {
    $package = Join-Path $fresh 'package'
    New-Item -ItemType Directory -Force -Path $package | Out-Null
    foreach ($item in @('package.json', 'lib', 'prebuilds')) {
        Copy-Item -Recurse -Force -LiteralPath (Join-Path $dir $item) -Destination $package
    }
    Push-Location $package
    try {
        # npm.cmd, never npm: PowerShell would pick npm.ps1.
        Invoke-Native 'npm.cmd' @('pkg', 'set', "version=$version")
        $previous = $ErrorActionPreference
        $ErrorActionPreference = 'Continue'
        try {
            $packed = & npm.cmd pack --silent --pack-destination $out
            $code = $LASTEXITCODE
        } finally {
            $ErrorActionPreference = $previous
        }
        if ($code -ne 0) {
            throw "npm.cmd failed with exit code $code"
        }
    } finally {
        Pop-Location
    }
    $tarball = @($packed | Where-Object { "$_".Trim() }) | Select-Object -Last 1
    $tarballPath = Join-Path $out $tarball
    Write-Host "packed $tarballPath"

    $project = Join-Path $fresh 'project'
    New-Item -ItemType Directory -Force -Path $project | Out-Null
    Push-Location $project
    try {
        Invoke-Native 'npm.cmd' @('init', '-y') | Out-Null
        Invoke-Native 'npm.cmd' @('install', '--offline', '--no-audit', '--no-fund', $tarballPath)
        Copy-Item -Force -LiteralPath (Join-Path $dir 'smoke.mjs') -Destination $project

        $installed = node -p "JSON.parse(require('fs').readFileSync('node_modules/@tuco86/weida/package.json', 'utf8')).version"
        if ($installed -ne $version) {
            throw "package.ps1: the installed package is $installed, not the workspace version $version"
        }

        $runtimes = @((Get-Command node, bun, deno).Source | ForEach-Object { Split-Path -Parent $_ } | Select-Object -Unique)
        $env:PATH = (@($runtimes) + @((Join-Path $env:SystemRoot 'System32'), $env:SystemRoot)) -join ';'
        foreach ($transport in @('inproc', 'quic')) {
            Invoke-Native 'node' @('smoke.mjs', $transport)
            Invoke-Native 'bun' @('smoke.mjs', $transport)
            Invoke-Native 'deno' @('run', '--allow-read', '--allow-env', '--allow-ffi', 'smoke.mjs', $transport)
        }
    } finally {
        $env:PATH = $oldPath
        Pop-Location
    }
} finally {
    Remove-Item -Recurse -Force -LiteralPath $fresh -ErrorAction SilentlyContinue
}
Write-Host "weida $version runs from its tarball under node, bun and deno: $tarballPath"
