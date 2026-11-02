// The one error class every failure of libweida throws (docs/c/README.md
// R9). The addon creates it with the message and sets the other fields from
// the thread-local record of the call that failed.
import native from "./native.js";

export class WeidaError extends Error {
  constructor(message) {
    super(message);
    this.name = "WeidaError";
    /** The R9 constant name, e.g. "WEIDA_EUNTRUSTED". */
    this.code = null;
    /** The positive status code. */
    this.errno = null;
    /** "WEIDA_LOSS_*" for WEIDA_ECONNECTION_LOST, otherwise null. */
    this.lossCause = null;
    /** The peer's fingerprint for WEIDA_EUNTRUSTED, otherwise null. */
    this.fingerprint = null;
    /** The connection's largest payload for WEIDA_ETOO_LARGE, otherwise null. */
    this.max = null;
    /** The raw OS error of WEIDA_EIO, when it carried one, otherwise null. */
    this.osCode = null;
  }
}

native.setErrorClass(WeidaError);

const ECLOSED = 103;

/** The error a dropped, spent or consumed object fails with. */
export function closed() {
  return native.error(ECLOSED);
}
