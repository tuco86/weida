# Compiles the Node-API addon prebuilds/win32-x64/weida.node with MSVC
# against crates/weida-c/include/weida.h and the weida.dll in
# $env:WEIDA_LIB_DIR, and copies that DLL beside it, where src/win32.c loads
# it from. The Windows twin of build.sh.
#
# Needs $env:NODE_API_DIR, a directory holding include\node\node_api.h (from
# node-v<version>-headers.tar.gz) and node.lib (from win-x64/node.lib on
# nodejs.org).
#
# Usage: $env:WEIDA_LIB_DIR = '<dir with weida.dll and weida.dll.lib>';
#        $env:NODE_API_DIR = '<dir>'; powershell -File build.ps1
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

if (-not $env:WEIDA_LIB_DIR) {
    throw 'build.ps1: set WEIDA_LIB_DIR to the directory holding weida.dll and weida.dll.lib'
}
$dll = Join-Path $env:WEIDA_LIB_DIR 'weida.dll'
$importLib = Join-Path $env:WEIDA_LIB_DIR 'weida.dll.lib'
foreach ($path in @($dll, $importLib)) {
    if (-not (Test-Path -LiteralPath $path)) {
        throw "build.ps1: $path does not exist"
    }
}
$napiInclude = $null
$nodeLib = $null
if ($env:NODE_API_DIR) {
    $napiInclude = Join-Path $env:NODE_API_DIR 'include\node'
    $nodeLib = Join-Path $env:NODE_API_DIR 'node.lib'
}
if (-not $env:NODE_API_DIR -or -not (Test-Path -LiteralPath (Join-Path $napiInclude 'node_api.h')) -or
    -not (Test-Path -LiteralPath $nodeLib)) {
    throw 'build.ps1: set NODE_API_DIR to a directory holding include\node\node_api.h and node.lib'
}

$target = node -p "process.platform + '-' + process.arch"
$out = Join-Path $dir "prebuilds\$target"
New-Item -ItemType Directory -Force -Path $out | Out-Null

# MSVC's environment from vcvars64.bat when cl.exe is not on PATH yet.
if ($null -eq (Get-Command cl.exe -ErrorAction SilentlyContinue)) {
    $vswhere = "${env:ProgramFiles(x86)}\Microsoft Visual Studio\Installer\vswhere.exe"
    $vs = $null
    if (Test-Path -LiteralPath $vswhere) {
        $vs = & $vswhere -latest -products * -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 -property installationPath
    }
    if (-not $vs) {
        throw 'build.ps1: no MSVC: vswhere found no Visual Studio with the x64 C++ tools'
    }
    $vcvars = Join-Path $vs 'VC\Auxiliary\Build\vcvars64.bat'
    $lines = & cmd.exe /s /c "`"$vcvars`" >nul && set"
    if ($LASTEXITCODE -ne 0) {
        throw "build.ps1: $vcvars failed with exit code $LASTEXITCODE"
    }
    foreach ($line in $lines) {
        $eq = $line.IndexOf('=')
        if ($eq -gt 0) {
            [Environment]::SetEnvironmentVariable($line.Substring(0, $eq), $line.Substring($eq + 1), 'Process')
        }
    }
}

$obj = Join-Path $env:TEMP "weida-js-build-$PID"
New-Item -ItemType Directory -Force -Path $obj | Out-Null
try {
    # /Fo ends in '/': a trailing backslash before the closing quote escapes
    # the quote on the Windows command line. /IMPLIB keeps weida-node.lib and
    # its .exp out of prebuilds. /MT links the C runtime statically, as
    # node-gyp's Release default does.
    Invoke-Native 'cl.exe' @(
        '/nologo', '/std:c11', '/O2', '/W3', '/WX', '/MT', '/LD',
        '/I', $napiInclude, '/I', (Join-Path $root 'crates\weida-c\include'),
        (Join-Path $dir 'src\addon.c'), (Join-Path $dir 'src\waiter.c'), (Join-Path $dir 'src\win32.c'),
        "/Fo$obj/", "/Fe$out\weida.node",
        '/link', "/IMPLIB:$obj\weida-node.lib", $importLib, $nodeLib,
        'ws2_32.lib', 'delayimp.lib', '/DELAYLOAD:node.exe', '/DELAYLOAD:weida.dll'
    )
} finally {
    Remove-Item -Recurse -Force -LiteralPath $obj -ErrorAction SilentlyContinue
}
Copy-Item -Force -LiteralPath $dll -Destination $out
Write-Host "built $out\weida.node against $dll"
