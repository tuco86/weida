"""Status codes to exceptions, and the failures that need no descriptor or
socket."""

import pytest

import weida
from weida._errors import check

CODES = list(range(1, 25)) + list(range(100, 105))


def test_every_code_raises_its_own_class_carrying_the_code():
    classes = set()
    for code in CODES:
        with pytest.raises(weida.WeidaError) as caught:
            check(-code)
        assert caught.value.code == code
        assert type(caught.value) is not weida.WeidaError, code
        classes.add(type(caught.value))
    assert len(classes) == len(CODES)
    assert issubclass(weida.Again, TimeoutError)
    assert issubclass(weida.InvalidArgument, ValueError)


def test_a_malformed_pin_is_an_invalid_fingerprint():
    with pytest.raises(weida.InvalidFingerprint):
        weida.Trust().add_pin("nope")


def test_a_runtime_with_no_worker_threads_is_a_runtime_failure():
    with pytest.raises(weida.RuntimeFailure):
        weida.Runtime.owned(weida.RuntimeConfig(worker_threads=0))
