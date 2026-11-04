// Waiting on libweida from the JS thread (docs/c/README.md R3, R4, R11):
// every native call tries with timeout 0; on AGAIN the handle's descriptor
// is watched once through the addon's waiter thread and the call is tried
// again when it turns readable. R11 allows one stale AGAIN, which simply
// watches again.
import native from "./native.js";
import { closed } from "./errors.js";

/** The pending watches of a handle object: a Set of entries. */
export const WAITS = Symbol("weida.waits");

function watch(owner, fd, signal, weak) {
  return new Promise((resolve, reject) => {
    const entry = {};
    const settle = () => {
      owner[WAITS].delete(entry);
      signal?.removeEventListener("abort", entry.abort);
    };
    entry.cancel = (reason) => {
      native.unwatch(entry.id);
      settle();
      reject(reason);
    };
    entry.abort = () => entry.cancel(signal.reason);
    entry.id = native.watch(
      fd,
      () => {
        settle();
        resolve();
      },
      weak === true,
    );
    owner[WAITS].add(entry);
    signal?.addEventListener("abort", entry.abort, { once: true });
  });
}

/**
 * Runs `attempt()` until it returns something other than AGAIN, watching
 * `fdOf()` in between. `owner` is the object whose `drop()` ends the wait;
 * aborting `signal` ends it with `signal.reason`. A `weak` wait does not keep
 * the process alive.
 */
export async function call(owner, attempt, fdOf, signal, weak = false) {
  signal?.throwIfAborted();
  let rc = attempt();
  while (rc === native.AGAIN) {
    await watch(owner, fdOf(), signal, weak);
    rc = attempt();
  }
  return rc;
}

/**
 * Awaits the op `start()` returns (R3b) and drops it on every path, which
 * cancels the operation when it did not finish (an abort, or `owner`
 * dropped).
 */
export async function op(owner, start, waitOf, fdOf, signal) {
  signal?.throwIfAborted();
  const handle = start();
  try {
    return await call(owner, () => waitOf(handle), () => fdOf(handle), signal);
  } finally {
    native.drop(handle);
  }
}

/**
 * Ends every pending wait of `owner` with WEIDA_ECLOSED, before its handle
 * is dropped or consumed, so no descriptor is watched once it is closed.
 */
export function cancelWaits(owner) {
  const pending = owner[WAITS];
  if (pending.size === 0) {
    return;
  }
  const error = closed();
  for (const entry of [...pending]) {
    entry.cancel(error);
  }
}
