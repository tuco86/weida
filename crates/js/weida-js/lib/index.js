// weida for JavaScript: the classes over the Node-API addon, one per C ABI
// handle (docs/libraries/weida-js.md). Every method that waits returns a
// Promise and takes an optional last `{ signal }`; `AbortSignal.timeout(ms)`
// is the timeout. A dropped or consumed object throws WEIDA_ECLOSED without
// calling C.
import native from "./native.js";
import { WeidaError } from "./errors.js";
import { WAITS, call, cancelWaits, op } from "./wait.js";

export { WeidaError };

const HANDLE = Symbol("weida.handle");
const dispose = Symbol.dispose ?? Symbol.for("Symbol.dispose");
const encoder = new TextEncoder();

/** The library's version, e.g. "0.1.0-alpha.11-dev". */
export function version() {
  return native.version();
}

/** The description of a status code, either sign; "unknown error" for one
 * libweida does not define. */
export function strerror(code) {
  return native.strerror(code);
}

function bytes(data) {
  if (typeof data === "string") {
    return encoder.encode(data);
  }
  if (data instanceof Uint8Array) {
    return data;
  }
  throw new TypeError("expected a Uint8Array or a string");
}

function handleOf(value, Class) {
  if (!(value instanceof Class)) {
    throw new TypeError(`expected a ${Class.name}`);
  }
  return value[HANDLE];
}

function init(object, handle) {
  object[HANDLE] = handle;
  object[WAITS] = new Set();
  return object;
}

/** An object of a class without a public constructor, around `handle`. */
function adopt(Class, handle) {
  return init(Object.create(Class.prototype), handle);
}

/** The handle of `object` for a consuming call (R6), its waits ended. */
function consume(object) {
  cancelWaits(object);
  return object[HANDLE];
}

function noConstructor(name, how) {
  throw new TypeError(`${name} has no public constructor; ${how} makes one`);
}

class Handle {
  /** Releases the handle now, ending its pending waits with WEIDA_ECLOSED. */
  drop() {
    cancelWaits(this);
    native.drop(this[HANDLE]);
  }

  [dispose]() {
    this.drop();
  }
}

function freezeMeta(meta) {
  return Object.freeze(meta);
}

/** A transfer meta `{ contentType? }` as a handle for one call. */
function withTransferMeta(meta, use) {
  if (meta == null) {
    return use(null);
  }
  const handle = native.transferMetaNew();
  try {
    if (meta.contentType != null) {
      native.transferMetaSetContentType(handle, meta.contentType);
    }
    return use(handle);
  } finally {
    native.drop(handle);
  }
}

/** `RuntimeConfig`: the fields set here, weida's defaults for the rest. */
export class RuntimeConfig {
  constructor({ workerThreads, shutdownTimeoutMs } = {}) {
    this.workerThreads = workerThreads;
    this.shutdownTimeoutMs = shutdownTimeoutMs;
  }
}

function configHandle(config) {
  if (config == null) {
    return null;
  }
  const handle = native.runtimeConfigNew();
  try {
    if (config.workerThreads != null) {
      native.runtimeConfigSetWorkerThreads(handle, config.workerThreads);
    }
    if (config.shutdownTimeoutMs != null) {
      native.runtimeConfigSetShutdownTimeoutMs(handle, config.shutdownTimeoutMs);
    }
  } catch (error) {
    native.drop(handle);
    throw error;
  }
  return handle;
}

/** A weida runtime: the reactor it owns, and every endpoint made from it. */
export class Runtime extends Handle {
  constructor() {
    super();
    noConstructor("Runtime", "Runtime.owned()");
  }

  /** A runtime on a reactor of its own. */
  static owned(config) {
    const cfg = configHandle(config);
    try {
      return adopt(Runtime, native.runtimeOwned(cfg));
    } finally {
      if (cfg !== null) {
        native.drop(cfg);
      }
    }
  }

  listener() {
    return adopt(Listener, native.runtimeListener(this[HANDLE]));
  }

  requester(clientTls) {
    return adopt(Requester, native.runtimeRequester(this[HANDLE], handleOf(clientTls, ClientTls)));
  }

  /** Closes every endpoint and waits for the connections to go idle.
   * Consumes the runtime. */
  async shutdown({ signal } = {}) {
    await op(
      this,
      () => native.runtimeShutdown(consume(this)),
      native.runtimeShutdownOpWait,
      native.runtimeShutdownOpFd,
      signal,
    );
  }
}

/** Whom a dialler trusts: what each address names, plus pins. */
export class Trust extends Handle {
  constructor() {
    super();
    init(this, native.trustNew());
  }

  /** Pins a `sha256:` fingerprint; returns this trust. */
  addPin(fingerprint) {
    native.trustAddPin(this[HANDLE], fingerprint);
    return this;
  }
}

/** A certificate chain and its private key. */
export class Identity extends Handle {
  constructor() {
    super();
    noConstructor("Identity", "Identity.generate() or Identity.fromPem()");
  }

  /** A self-signed identity with a fresh key, for pinning. */
  static generate() {
    return adopt(Identity, native.identityGenerate());
  }

  /** An identity from PEM: the chain, leaf first, and the key. */
  static fromPem(cert, key) {
    return adopt(Identity, native.identityFromPem(bytes(cert), bytes(key)));
  }

  /** The public-key fingerprint, `sha256:` text. */
  fingerprint() {
    return native.identityFingerprint(this[HANDLE]);
  }
}

/** The TLS terms a requester dials on: anonymous, under `trust`. */
export class ClientTls extends Handle {
  constructor(trust) {
    super();
    init(this, native.clientTlsNew(handleOf(trust, Trust)));
  }
}

/** The TLS terms a QUIC binding serves on, presenting `identity`. */
export class ServerTls extends Handle {
  constructor(identity) {
    super();
    init(this, native.serverTlsNew(handleOf(identity, Identity)));
  }
}

/** Where a runtime serves: bindings and the endpoints registered on them. */
export class Listener extends Handle {
  constructor() {
    super();
    noConstructor("Listener", "Runtime.listener()");
  }

  /** Serves QUIC on `addr`, e.g. "127.0.0.1:0". */
  async bindQuic(addr, serverTls, { signal } = {}) {
    const tls = handleOf(serverTls, ServerTls);
    const binding = await op(
      this,
      () => native.listenerBindQuic(this[HANDLE], addr, tls),
      native.listenerBindQuicOpWait,
      native.listenerBindQuicOpFd,
      signal,
    );
    return adopt(Binding, binding);
  }

  /** Answers on the in-process bus `bus`. */
  bindInproc(bus) {
    return adopt(LocalBinding, native.listenerBindInproc(this[HANDLE], bus));
  }

  /** The replier serving `path` on every binding of this listener. */
  replier(path) {
    return adopt(Replier, native.listenerReplier(this[HANDLE], path));
  }
}

/** A QUIC binding: the socket a listener serves on. */
export class Binding extends Handle {
  constructor() {
    super();
    noConstructor("Binding", "Listener.bindQuic()");
  }

  localAddr() {
    return native.bindingLocalAddr(this[HANDLE]);
  }
}

/** An in-process binding: the bus a listener answers on. */
export class LocalBinding extends Handle {
  constructor() {
    super();
    noConstructor("LocalBinding", "Listener.bindInproc()");
  }

  bus() {
    return native.localBindingBus(this[HANDLE]);
  }
}

/** The requesting half of Req/Rep: dials repliers and sends them requests. */
export class Requester extends Handle {
  constructor() {
    super();
    noConstructor("Requester", "Runtime.requester()");
  }

  /** Dials `url`; the runtime redials it when the connection is lost. */
  async connect(url, { signal } = {}) {
    await op(
      this,
      () => native.requesterConnect(this[HANDLE], url),
      native.requesterConnectOpWait,
      native.requesterConnectOpFd,
      signal,
    );
  }

  /** Sends `body` (a string is sent as UTF-8) and resolves with the reply as
   * soon as its header arrives. Aborting abandons the exchange. */
  async request(body, { signal } = {}) {
    const data = bytes(body);
    const transfer = await op(
      this,
      () => native.requesterRequest(this[HANDLE], data),
      native.requesterRequestOpWait,
      native.requesterRequestOpFd,
      signal,
    );
    return adopt(IncomingTransfer, transfer);
  }
}

/** The replying half of Req/Rep, serving one endpoint path. */
export class Replier extends Handle {
  constructor() {
    super();
    noConstructor("Replier", "Listener.replier()");
  }

  path() {
    return native.replierPath(this[HANDLE]);
  }

  /** The next request, as soon as its header arrives. */
  async accept({ signal } = {}) {
    const handle = this[HANDLE];
    const request = await call(
      this,
      () => native.replierAccept(handle),
      () => native.replierFd(handle),
      signal,
    );
    return adopt(IncomingRequest, request);
  }
}

/** One accepted request: its body and the right to reply. */
export class IncomingRequest extends Handle {
  constructor() {
    super();
    noConstructor("IncomingRequest", "Replier.accept()");
  }

  meta() {
    return freezeMeta(native.incomingRequestMeta(this[HANDLE]));
  }

  /** Detaches the body, to be read like a socket; once. */
  takeBody() {
    return adopt(IncomingTransfer, native.incomingRequestTakeBody(this[HANDLE]));
  }

  /** Opens the reply stream with `meta` (`{ contentType? }`). Consumes the
   * request. */
  async reply(meta, { signal } = {}) {
    const transfer = await op(
      this,
      () => withTransferMeta(meta, (m) => native.incomingRequestReply(consume(this), m)),
      native.incomingRequestReplyOpWait,
      native.incomingRequestReplyOpFd,
      signal,
    );
    return adopt(OutgoingTransfer, transfer);
  }
}

/** An inbound transfer: a payload read like a socket. */
export class IncomingTransfer extends Handle {
  constructor() {
    super();
    noConstructor("IncomingTransfer", "Requester.request() or IncomingRequest.takeBody()");
  }

  meta() {
    return freezeMeta(native.incomingTransferMeta(this[HANDLE]));
  }

  /** Up to `cap` bytes; an empty array at the end of the payload. */
  async read(cap = 65536, { signal } = {}) {
    const handle = this[HANDLE];
    return call(
      this,
      () => native.incomingTransferRead(handle, cap),
      () => native.incomingTransferFd(handle),
      signal,
    );
  }

  /** The whole remaining payload, refusing more than `maxBytes`. Consumes
   * the transfer. */
  async collect(maxBytes, { signal } = {}) {
    const message = await op(
      this,
      () => native.incomingTransferCollect(consume(this), maxBytes),
      native.incomingTransferCollectOpWait,
      native.incomingTransferCollectOpFd,
      signal,
    );
    return Object.freeze({ payload: message.payload, meta: freezeMeta(message.meta) });
  }
}

/** An outbound transfer: a payload written like a socket. */
export class OutgoingTransfer extends Handle {
  constructor() {
    super();
    noConstructor("OutgoingTransfer", "IncomingRequest.reply()");
  }

  /** Writes some of `data`; resolves with how many bytes flow control took. */
  async write(data, { signal } = {}) {
    const buffer = bytes(data);
    const handle = this[HANDLE];
    return call(
      this,
      () => native.outgoingTransferWrite(handle, buffer),
      () => native.outgoingTransferFd(handle),
      signal,
    );
  }

  /** Writes all of `data`: the loop the C ABI leaves to the caller (R3c). */
  async writeAll(data, options) {
    let rest = bytes(data);
    while (rest.length > 0) {
      rest = rest.subarray(await this.write(rest, options));
    }
  }

  /** Ends the payload and returns its delivery receipt. Consumes the
   * transfer. */
  finish() {
    return adopt(Delivery, native.outgoingTransferFinish(consume(this)));
  }
}

/** The receipt of a finished outbound transfer. */
export class Delivery extends Handle {
  constructor() {
    super();
    noConstructor("Delivery", "OutgoingTransfer.finish()");
  }

  /** Waits for the peer's transport to acknowledge the whole payload.
   * Consumes the receipt. */
  async delivered({ signal } = {}) {
    await op(
      this,
      () => native.deliveryDelivered(consume(this)),
      native.deliveryDeliveredOpWait,
      native.deliveryDeliveredOpFd,
      signal,
    );
  }
}
