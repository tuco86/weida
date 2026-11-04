"""Identities as PEM and DER, the `cryptography` conversions with and without
the package, and the argument errors of the identity and TLS values."""

import hashlib
import ipaddress
import sys

import pytest

import weida

# `cryptography`, where it is installed, is imported at collection, so its
# first import (compiling bytecode in a fresh venv) is not charged to one
# test's second.
try:
    import cryptography.hazmat.primitives.serialization  # noqa: F401
    import cryptography.x509  # noqa: F401
except ImportError:
    pass


def test_to_pem_reads_back_with_the_same_fingerprint():
    identity = weida.Identity.generate()
    pem = identity.to_pem()
    assert isinstance(pem, bytearray)
    assert weida.Identity.from_pem(pem, pem).fingerprint() == identity.fingerprint()
    assert b"PRIVATE KEY" in pem
    chain = identity.certificate_pem()
    assert isinstance(chain, bytes)
    assert b"-----BEGIN CERTIFICATE-----" in chain
    assert b"PRIVATE KEY" not in chain


def test_certificates_are_the_identitys_key():
    x509 = pytest.importorskip("cryptography.x509")
    from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

    identity = weida.Identity.generate()
    certificates = identity.certificates()
    assert len(certificates) == 1
    spki = (
        x509.load_der_x509_certificate(certificates[0])
        .public_key()
        .public_bytes(Encoding.DER, PublicFormat.SubjectPublicKeyInfo)
    )
    assert "sha256:" + hashlib.sha256(spki).hexdigest() == identity.fingerprint()


def test_generate_for_names_the_certificate():
    x509 = pytest.importorskip("cryptography.x509")

    identity = weida.Identity.generate_for(["localhost", "127.0.0.1"])
    leaf = x509.load_der_x509_certificate(identity.certificates()[0])
    names = leaf.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
    assert names.get_values_for_type(x509.DNSName) == ["localhost"]
    assert names.get_values_for_type(x509.IPAddress) == [ipaddress.ip_address("127.0.0.1")]
    assert weida.Identity.generate_for([]).fingerprint().startswith("sha256:")


def test_cryptography_objects_make_the_same_identity():
    x509 = pytest.importorskip("cryptography.x509")
    from cryptography.hazmat.primitives import serialization

    identity = weida.Identity.generate()
    key = serialization.load_pem_private_key(bytes(identity.to_pem()), None)
    certs = [x509.load_der_x509_certificate(der) for der in identity.certificates()]
    assert weida.Identity.from_cryptography(certs, key).fingerprint() == identity.fingerprint()
    assert (
        weida.Identity.from_cryptography(certs[0], identity.to_pem()).fingerprint()
        == identity.fingerprint()
    )
    with pytest.raises(TypeError):
        weida.Identity.from_cryptography(certs[0], key.public_key())


def test_without_cryptography_bytes_convert_and_objects_are_type_errors(monkeypatch):
    names = ["cryptography"] + [name for name in sys.modules if name.startswith("cryptography.")]
    for name in names:
        monkeypatch.setitem(sys.modules, name, None)
    identity = weida.Identity.generate()
    copy = weida.Identity.from_cryptography(identity.certificate_pem(), identity.to_pem())
    assert copy.fingerprint() == identity.fingerprint()
    with pytest.raises(TypeError):
        weida.Identity.from_cryptography(object(), identity.to_pem())
    with pytest.raises(TypeError):
        weida.Trust().add_anchor(object())


def test_argument_errors():
    identity = weida.Identity.generate()
    with pytest.raises(TypeError):
        weida.Identity.generate_for("localhost")
    with pytest.raises(TypeError):
        weida.Identity.generate_for([1])
    with pytest.raises(TypeError):
        weida.Trust().add_anchor("ca.pem")
    with pytest.raises(TypeError):
        weida.Identity.from_pem_file(1)
    with pytest.raises(TypeError):
        weida.ServerTls(identity, require_client="any-key")
    with pytest.raises(TypeError):
        weida.ClientTls(weida.Trust(), identity="x")
    with pytest.raises(ValueError):
        weida.Identity.generate_for(["a\0b"])
    with pytest.raises(ValueError):
        weida.Identity.from_cryptography([], identity.to_pem())
    with pytest.raises(ValueError):
        weida.Identity.from_pem_file("a\0b")
    weida.ServerTls(identity, require_client=weida.ANY_KEY)
    weida.ServerTls(identity, require_client=weida.Trust())
