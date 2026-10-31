"""Build hooks: the version from the Cargo workspace, and libweida plus the
cffi module that loads it, built into `src/weida/` before the wheel is
assembled."""

import os
import shutil
import subprocess
import sys
import sysconfig

from hatchling.builders.hooks.plugin.interface import BuildHookInterface
from hatchling.metadata.plugin.interface import MetadataHookInterface

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
PACKAGE = os.path.join(HERE, "src", "weida")


def pep440(version: str) -> str:
    """PEP 440 spelling of a Cargo version: `0.1.0` stays, `0.1.0-alpha.6`
    is `0.1.0a6`, and a trailing `-dev` adds `.dev0`."""
    dev = version.endswith("-dev")
    if dev:
        version = version[: -len("-dev")]
    base, _, pre = version.partition("-")
    if pre:
        kind, number = pre.split(".")
        base += {"alpha": "a", "beta": "b", "rc": "rc"}[kind] + number
    return base + ".dev0" if dev else base


def workspace_version() -> str:
    if sys.version_info >= (3, 11):
        import tomllib
    else:
        import tomli as tomllib
    with open(os.path.join(ROOT, "Cargo.toml"), "rb") as manifest:
        return tomllib.load(manifest)["workspace"]["package"]["version"]


def library_name() -> str:
    if sys.platform == "win32":
        return "weida.dll"
    if sys.platform == "darwin":
        return "libweida.dylib"
    return "libweida.so"


class CustomMetadataHook(MetadataHookInterface):
    def update(self, metadata: dict) -> None:
        metadata["version"] = pep440(workspace_version())


class CustomBuildHook(BuildHookInterface):
    def initialize(self, version: str, build_data: dict) -> None:
        import cffi

        target = os.environ.get("CARGO_TARGET_DIR") or os.path.join(ROOT, "target")
        env = dict(os.environ, CARGO_TARGET_DIR=target)
        subprocess.run(
            ["cargo", "build", "--release", "--locked", "-p", "weida-c"],
            cwd=ROOT,
            env=env,
            check=True,
        )
        name = library_name()
        shutil.copy2(os.path.join(target, "release", name), os.path.join(PACKAGE, name))

        ffi = cffi.FFI()
        with open(os.path.join(PACKAGE, "_cdef.h")) as cdef:
            ffi.cdef(cdef.read())
        ffi.set_source("weida._ffi", None)
        ffi.emit_python_code(os.path.join(PACKAGE, "_ffi.py"))

        build_data["pure_python"] = False
        build_data["infer_tag"] = False
        platform = sysconfig.get_platform().replace("-", "_").replace(".", "_")
        build_data["tag"] = "py3-none-" + platform
