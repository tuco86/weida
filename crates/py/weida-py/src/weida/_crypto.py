"""The optional `cryptography` package's certificates and private keys as PEM
(docs/decisions/0047). Imported on each call, so weida loads and works
without it."""

import importlib
import os
import types
from typing import Optional

BYTES = (bytes, bytearray, memoryview)


def _cryptography() -> Optional[types.SimpleNamespace]:
    """The `cryptography` modules weida converts from, or `None` without the
    package. Not cached, so hiding it through `sys.modules` takes effect."""
    try:
        return types.SimpleNamespace(
            x509=importlib.import_module("cryptography.x509"),
            serialization=importlib.import_module("cryptography.hazmat.primitives.serialization"),
            rsa=importlib.import_module("cryptography.hazmat.primitives.asymmetric.rsa"),
            ec=importlib.import_module("cryptography.hazmat.primitives.asymmetric.ec"),
            ed25519=importlib.import_module("cryptography.hazmat.primitives.asymmetric.ed25519"),
        )
    except ImportError:
        return None


def certificate_pem(cert):
    """PEM for PEM bytes (returned unchanged), one `cryptography` certificate,
    or a list or tuple of them, leaf first."""
    if isinstance(cert, BYTES):
        return cert
    if isinstance(cert, (list, tuple)) and not cert:
        raise ValueError("expected at least one certificate")
    c = _cryptography()
    if c is not None:
        if isinstance(cert, c.x509.Certificate):
            return cert.public_bytes(c.serialization.Encoding.PEM)
        if isinstance(cert, (list, tuple)) and all(isinstance(x, c.x509.Certificate) for x in cert):
            return b"".join(x.public_bytes(c.serialization.Encoding.PEM) for x in cert)
    raise TypeError(
        "expected PEM bytes or a cryptography x509.Certificate (or a list of them, leaf first)"
    )


def key_pem(key):
    """PEM for PEM bytes (returned unchanged) or a `cryptography` RSA, EC or
    Ed25519 private key, exported as unencrypted PKCS#8."""
    if isinstance(key, BYTES):
        return key
    c = _cryptography()
    if c is not None and isinstance(
        key, (c.rsa.RSAPrivateKey, c.ec.EllipticCurvePrivateKey, c.ed25519.Ed25519PrivateKey)
    ):
        s = c.serialization
        return key.private_bytes(s.Encoding.PEM, s.PrivateFormat.PKCS8, s.NoEncryption())
    raise TypeError("expected PEM bytes or an RSA, EC or Ed25519 private key from cryptography")


def path(value) -> bytes:
    """A `str`, `bytes` or `os.PathLike` path as the bytes the C ABI takes. A
    path that is not UTF-8 reaches the ABI, which refuses it with
    `InvalidArgument`."""
    encoded = os.fsencode(value)
    if b"\0" in encoded:
        raise ValueError("embedded null byte")
    return encoded
