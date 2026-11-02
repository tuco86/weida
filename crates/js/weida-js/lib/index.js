// weida for JavaScript: the classes over the Node-API addon, one per C ABI
// handle (docs/libraries/weida-js.md). Every method that waits returns a
// Promise and takes an optional last `{ signal }`; `AbortSignal.timeout(ms)`
// is the timeout. A dropped or consumed object throws WEIDA_ECLOSED without
// calling C.
// A namespace import, so the module still loads on a runtime that lacks one
// of the exports; the conversions that need it check at use.
import * as nodeCrypto from "node:crypto";
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

/** Whether `value` is an instance of `node:crypto`'s export `name`. */
function isCrypto(value, name) {
  const Class = nodeCrypto[name];
  return typeof Class === "function" && value instanceof Class;
}

/** An `X509Certificate` from PEM or DER. */
function x509(data) {
  const X509 = nodeCrypto.X509Certificate;
  if (typeof X509 !== "function") {
    throw new TypeError("node:crypto has no X509Certificate on this runtime");
  }
  return new X509(data);
}

/** A certificate chain as PEM: X509Certificate(s), bytes or a string. */
function certPem(cert) {
  if (isCrypto(cert, "X509Certificate")) {
    return cert.toString();
  }
  if (Array.isArray(cert)) {
    if (cert.length === 0) {
      throw new TypeError("expected at least one certificate");
    }
    return cert
      .map((c) => {
        if (!isCrypto(c, "X509Certificate")) {
          throw new TypeError("expected an array of X509Certificate");
        }
        return c.toString();
      })
      .join("\n");
  }
  return bytes(cert);
}

/** A private key as PEM: a private KeyObject (exported as PKCS#8), bytes or
 * a string. */
function keyPem(key) {
  if (isCrypto(key, "KeyObject")) {
    if (key.type !== "private") {
      throw new TypeError("expected a private KeyObject");
    }
    return key.export({ type: "pkcs8", format: "pem" });
  }
  return bytes(key);
}

/** The certificate blocks of a PEM text. */
function pemBlocks(text) {
  return text.match(/-----BEGIN CERTIFICATE-----[\s\S]+?-----END CERTIFICATE-----\n?/g) ?? [];
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

/** Freezes metadata, the presented chain mapped to `X509Certificate`s. */
function freezeMeta(meta) {
  if (meta.peerChain != null) {
    meta.peerChain = Object.freeze(meta.peerChain.map((der) => x509(der)));
  }
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

  /** Trusts the certificate authorities in `anchor`, an `X509Certificate`
   * or PEM bytes, for a peer whose chain leads to one and names the dialled
   * host; returns this trust. */
  addAnchor(anchor) {
    const pem = isCrypto(anchor, "X509Certificate") ? anchor.toString() : anchor;
    native.trustAddAnchor(this[HANDLE], bytes(pem));
    return this;
  }

  /** Trusts the certificate authorities in a PEM file, read at each dial;
   * returns this trust. */
  addAnchorFile(path) {
    native.trustAddAnchorFile(this[HANDLE], path);
    return this;
  }
}

/** A certificate chain and its private key. Conversions from Node's key
 * types follow docs/decisions/0047. */
export class Identity extends Handle {
  constructor() {
    super();
    noConstructor("Identity", "Identity.generate(), Identity.from() or Identity.fromPem()");
  }

  /** A self-signed identity with a fresh key, for pinning. */
  static generate() {
    return adopt(Identity, native.identityGenerate());
  }

  /** A self-signed identity whose certificate names `names` (DNS names or IP
   * literals), so a peer anchoring it can check the host it dialled. */
  static generateFor(names) {
    if (!Array.isArray(names) || !names.every((n) => typeof n === "string")) {
      throw new TypeError("expected an array of names");
    }
    return adopt(Identity, native.identityGenerateFor(names));
  }

  /** An identity from PEM: the chain, leaf first, and the key. */
  static fromPem(cert, key) {
    return adopt(Identity, native.identityFromPem(bytes(cert), bytes(key)));
  }

  /** An identity from Node's types or PEM: `cert` is an `X509Certificate`,
   * an array of them (leaf first) or PEM bytes; `key` a private `KeyObject`
   * or PEM bytes. */
  static from({ cert, key } = {}) {
    if (cert == null || key == null) {
      throw new TypeError("Identity.from needs { cert, key }");
    }
    return Identity.fromPem(certPem(cert), keyPem(key));
  }

  /** An identity from one PEM file holding the chain and the key, the form
   * `toPem()` writes. The file is read at first use. */
  static fromPemFile(path) {
    return adopt(Identity, native.identityFromPemFile(path));
  }

  /** An identity from a chain file and a key file, read at first use. */
  static fromPemFiles(cert, key) {
    return adopt(Identity, native.identityFromPemFiles(cert, key));
  }

  /** The public-key fingerprint, `sha256:` text. */
  fingerprint() {
    return native.identityFingerprint(this[HANDLE]);
  }

  /** The certificate chain as PEM, without the key. */
  certificatePem() {
    return native.identityCertificatePem(this[HANDLE]);
  }

  /** The chain and the private key as one PEM document. Holds the private
   * key; wipe it with `fill(0)` when done. */
  toPem() {
    return native.identityToPem(this[HANDLE]);
  }

  /** The certificate chain, leaf first. */
  certificates() {
    return pemBlocks(this.certificatePem()).map((block) => x509(block));
  }
}

/** The TLS terms a requester dials on, under `trust`: anonymous, or
 * presenting `identity`. */
export class ClientTls extends Handle {
  constructor(trust, { identity } = {}) {
    super();
    const trustHandle = handleOf(trust, Trust);
    const id = identity == null ? null : handleOf(identity, Identity);
    const handle = native.clientTlsNew(trustHandle);
    try {
      if (id !== null) {
        native.clientTlsSetIdentity(handle, id);
      }
    } catch (error) {
      native.drop(handle);
      throw error;
    }
    init(this, handle);
  }
}

/** The TLS terms a QUIC binding serves on, presenting `identity`. With
 * `requireClient` every client must present a key: one the `Trust` accepts,
 * or with "any-key" any key it proves it holds. */
export class ServerTls extends Handle {
  constructor(identity, { requireClient } = {}) {
    super();
    const id = handleOf(identity, Identity);
    if (!(requireClient == null || requireClient === "any-key" || requireClient instanceof Trust)) {
      throw new TypeError('requireClient is a Trust or "any-key"');
    }
    const handle = native.serverTlsNew(id);
    try {
      if (requireClient === "any-key") {
        native.serverTlsRequireAnyKey(handle);
      } else if (requireClient != null) {
        native.serverTlsRequireClient(handle, requireClient[HANDLE]);
      }
    } catch (error) {
      native.drop(handle);
      throw error;
    }
    init(this, handle);
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
