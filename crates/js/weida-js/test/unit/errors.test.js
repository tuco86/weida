// Status codes to errors, and the failures that need no descriptor.
import assert from "node:assert/strict";
import { test } from "node:test";

import * as weida from "../../lib/index.js";
import native from "../../lib/native.js";

const UNIT = { timeout: 1000 };

// docs/c/README.md R9: the 24 variants and the ABI's own five codes.
const R9 = [
  [1, "WEIDA_ERUNTIME"],
  [2, "WEIDA_EINVALID_ADDRESS"],
  [3, "WEIDA_EINVALID_ENDPOINT_PATH"],
  [4, "WEIDA_EINVALID_FINGERPRINT"],
  [5, "WEIDA_EALREADY_REGISTERED"],
  [6, "WEIDA_ENOT_CONNECTED"],
  [7, "WEIDA_ECONNECTION_LOST"],
  [8, "WEIDA_ENEGOTIATION"],
  [9, "WEIDA_EPROTOCOL"],
  [10, "WEIDA_EREJECTED"],
  [11, "WEIDA_EUNKNOWN_ENDPOINT"],
  [12, "WEIDA_EUNSUPPORTED"],
  [13, "WEIDA_ENO_PARKED_CONNECTION"],
  [14, "WEIDA_ENO_REPLY"],
  [15, "WEIDA_ECANCELED"],
  [16, "WEIDA_EEXPIRED"],
  [17, "WEIDA_EINDETERMINATE"],
  [18, "WEIDA_ELIMIT_EXCEEDED"],
  [19, "WEIDA_EDATAGRAMS_UNAVAILABLE"],
  [20, "WEIDA_ETOO_LARGE"],
  [21, "WEIDA_ETLS"],
  [22, "WEIDA_EUNTRUSTED"],
  [23, "WEIDA_EIO"],
  [24, "WEIDA_ETRANSPORT"],
  [100, "WEIDA_EAGAIN"],
  [101, "WEIDA_EINTR"],
  [102, "WEIDA_EINVAL"],
  [103, "WEIDA_ECLOSED"],
  [104, "WEIDA_EPANIC"],
];

test("every status code is a WeidaError carrying its R9 name", UNIT, () => {
  for (const [errno, code] of R9) {
    const error = native.error(-errno);
    assert.ok(error instanceof weida.WeidaError, code);
    assert.equal(error.code, code);
    assert.equal(error.errno, errno);
    assert.equal(error.message, weida.strerror(errno));
  }
});

test("strerror of an unknown code is 'unknown error'", UNIT, () => {
  assert.equal(weida.strerror(9999), "unknown error");
  assert.equal(weida.strerror(-9999), "unknown error");
});

test("a runtime with no worker threads is a runtime failure", UNIT, () => {
  assert.throws(() => weida.Runtime.owned(new weida.RuntimeConfig({ workerThreads: 0 })), {
    name: "WeidaError",
    code: "WEIDA_ERUNTIME",
  });
});

test("a malformed pin is an invalid fingerprint", UNIT, () => {
  assert.throws(() => new weida.Trust().addPin("nope"), {
    code: "WEIDA_EINVALID_FINGERPRINT",
    errno: 4,
  });
});
