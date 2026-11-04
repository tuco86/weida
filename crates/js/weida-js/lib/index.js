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

/** A fresh root trace context as W3C `traceparent` text, for
 * `TransferMeta.trace`: weida never starts a trace on its own. */
export function newTrace() {
  return native.newTrace();
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
  if (meta.peerLocal != null) Object.freeze(meta.peerLocal);
  return Object.freeze(meta);
}

/** Freezes one connection record and the objects inside it. */
function freezeStats(record) {
  if (record.transport !== null) {
    Object.freeze(record.transport.path);
    Object.freeze(record.transport.tx);
    Object.freeze(record.transport.rx);
    Object.freeze(record.transport);
  }
  if (record.remote !== null) {
    Object.freeze(record.remote.tx);
    Object.freeze(record.remote.rx);
    Object.freeze(record.remote);
  }
  return Object.freeze(record);
}

/** The value of the weida.h constant `name`, which must start with
 * `prefix`: enumerations travel as the constant names. */
function constant(prefix, name) {
  const value =
    typeof name === "string" && name.startsWith(prefix) ? native.CONSTANTS[name] : undefined;
  if (value === undefined) {
    const names = Object.keys(native.CONSTANTS).filter((k) => k.startsWith(prefix));
    throw new TypeError(`expected one of ${names.join(", ")}`);
  }
  return value;
}

/** Throws for an own key of `object` that is not in `known`. */
function checkKeys(object, known, what) {
  for (const key of Object.keys(object)) {
    if (!known.includes(key)) {
      throw new TypeError(`unknown ${what} field: ${key}`);
    }
  }
}

// Field tables: [jsKey, setter, kind], the kind "value" (passed as is) or
// the prefix of an enumeration's constants.
const CONFIG_FIELDS = [
  ["workerThreads", native.runtimeConfigSetWorkerThreads, "value"],
  ["shutdownTimeoutMs", native.runtimeConfigSetShutdownTimeoutMs, "value"],
  ["maxConnections", native.runtimeConfigSetMaxConnections, "value"],
  ["maxConnectionsPerPeer", native.runtimeConfigSetMaxConnectionsPerPeer, "value"],
  ["endpointQueue", native.runtimeConfigSetEndpointQueue, "value"],
  ["maxResolvedAddresses", native.runtimeConfigSetMaxResolvedAddresses, "value"],
  ["connectAttemptTimeoutMs", native.runtimeConfigSetConnectAttemptTimeoutMs, "value"],
  ["sendTimeoutMs", native.runtimeConfigSetSendTimeoutMs, "value"],
  ["outboxMessages", native.runtimeConfigSetOutboxMessages, "value"],
  ["outboxBytes", native.runtimeConfigSetOutboxBytes, "value"],
  ["maxHandshakes", native.runtimeConfigSetMaxHandshakes, "value"],
  ["retryAboveHandshakes", native.runtimeConfigSetRetryAboveHandshakes, "value"],
  ["outboxFull", native.runtimeConfigSetOutboxFull, "WEIDA_OUTBOX_FULL_"],
  ["discovery", native.runtimeConfigSetDiscovery, "WEIDA_DISCOVERY_"],
];

const LIMITS_FIELDS = [
  ["maxHeaderBytes", native.limitsSetMaxHeaderBytes, "value"],
  ["maxConcurrentUniStreams", native.limitsSetMaxConcurrentUniStreams, "value"],
  ["maxConcurrentBidiStreams", native.limitsSetMaxConcurrentBidiStreams, "value"],
  ["streamReceiveWindow", native.limitsSetStreamReceiveWindow, "value"],
  ["connectionReceiveWindow", native.limitsSetConnectionReceiveWindow, "value"],
  ["keepAliveMs", native.limitsSetKeepAliveMs, "value"],
  ["idleTimeoutMs", native.limitsSetIdleTimeoutMs, "value"],
  ["helloTimeoutMs", native.limitsSetHelloTimeoutMs, "value"],
  ["maxSubscriptions", native.limitsSetMaxSubscriptions, "value"],
  ["subscriberBufferBytes", native.limitsSetSubscriberBufferBytes, "value"],
  ["maxSequenceScopes", native.limitsSetMaxSequenceScopes, "value"],
  ["maxReorderHold", native.limitsSetMaxReorderHold, "value"],
  ["maxLocalStreams", native.limitsSetMaxLocalStreams, "value"],
  ["maxParkedReverse", native.limitsSetMaxParkedReverse, "value"],
  ["maxDedupEntries", native.limitsSetMaxDedupEntries, "value"],
  ["datagramReceiveBytes", native.limitsSetDatagramReceiveBytes, "value"],
  ["datagramSendBytes", native.limitsSetDatagramSendBytes, "value"],
  ["pathReport", native.limitsSetPathReport, "value"],
  ["maxFlows", native.limitsSetMaxFlows, "value"],
  ["flowQueueBytes", native.limitsSetFlowQueueBytes, "value"],
  ["flowEarlyBytes", native.limitsSetFlowEarlyBytes, "value"],
  ["flowEarlyHoldMs", native.limitsSetFlowEarlyHoldMs, "value"],
  ["congestion", native.limitsSetCongestion, "WEIDA_CONGESTION_"],
  ["headerTimeoutMs", native.limitsSetHeaderTimeoutMs, "value"],
  ["maxHeaderStreams", native.limitsSetMaxHeaderStreams, "value"],
];

const GUARANTEE_FIELDS = [
  ["delivery", native.guaranteeSetSetDelivery, "WEIDA_DELIVERY_LEVEL_"],
  ["acknowledgement", native.guaranteeSetSetAcknowledgement, "WEIDA_ACKNOWLEDGEMENT_"],
  ["durability", native.guaranteeSetSetDurability, "WEIDA_DURABILITY_"],
  ["replicas", native.guaranteeSetSetReplicas, "value"],
  ["ordering", native.guaranteeSetSetOrdering, "WEIDA_ORDERING_MODE_"],
  ["deduplication", native.guaranteeSetSetDeduplication, "WEIDA_DEDUPLICATION_"],
  ["dedupWindowMs", native.guaranteeSetSetDedupWindowMs, "value"],
  ["backpressure", native.guaranteeSetSetBackpressure, "WEIDA_BACKPRESSURE_"],
  ["producerNaming", native.guaranteeSetSetProducerNaming, "WEIDA_PRODUCER_NAMING_"],
  ["controlIsolated", native.guaranteeSetSetControlIsolated, "value"],
];

const RECONNECT_FIELDS = [
  ["initialMs", native.reconnectPolicySetInitialMs, "value"],
  ["maxMs", native.reconnectPolicySetMaxMs, "value"],
  ["jitter", native.reconnectPolicySetJitter, "value"],
  ["maxAttempts", native.reconnectPolicySetMaxAttempts, "value"],
  ["stopOnPeerClosed", native.reconnectPolicySetStopOnPeerClosed, "value"],
];

const NESTED = ["limits", "guarantees", "reconnect"];
const CONFIG_KEYS = [...CONFIG_FIELDS.map(([key]) => key), ...NESTED];

/** Checks `object`'s keys against `fields` (and `extra`), then calls the
 * setter of each field that is neither null nor undefined. */
function applyFields(handle, object, fields, what, extra = []) {
  checkKeys(object, [...fields.map(([key]) => key), ...extra], what);
  for (const [key, setter, kind] of fields) {
    const value = object[key];
    if (value != null) {
      setter(handle, kind === "value" ? value : constant(kind, value));
    }
  }
}

/** Builds a nested configuration handle from `object`, hands it to `use`
 * (the parent's setter, which copies it) and drops it. */
function nested(object, make, fields, what, use, extra) {
  const handle = make();
  try {
    applyFields(handle, object, fields, what, extra);
    use(handle);
  } finally {
    native.drop(handle);
  }
}

const META_FIELDS = [
  ["contentType", native.transferMetaSetContentType, "value"],
  ["contentLen", native.transferMetaSetContentLen, "value"],
  ["trace", native.transferMetaSetTrace, "value"],
  ["topic", native.transferMetaSetTopic, "value"],
  ["achieved", native.transferMetaSetAchieved, "WEIDA_ACKNOWLEDGEMENT_"],
];

/** A transfer meta `{ contentType?, contentLen?, trace?, topic?,
 * achieved? }` as a handle for one call. */
function withTransferMeta(meta, use) {
  if (meta == null) {
    return use(null);
  }
  const handle = native.transferMetaNew();
  try {
    applyFields(handle, meta, META_FIELDS, "TransferMeta");
    return use(handle);
  } finally {
    native.drop(handle);
  }
}

/** `RuntimeConfig`: the fields set here, weida's defaults for the rest. */
export class RuntimeConfig {
  constructor(fields = {}) {
    checkKeys(fields, CONFIG_KEYS, "RuntimeConfig");
    for (const key of CONFIG_KEYS) {
      if (fields[key] !== undefined) {
        this[key] = fields[key];
      }
    }
  }
}

function configHandle(config) {
  if (config == null) {
    return null;
  }
  const handle = native.runtimeConfigNew();
  try {
    applyFields(handle, config, CONFIG_FIELDS, "RuntimeConfig", NESTED);
    if (config.limits != null) {
      nested(config.limits, native.limitsNew, LIMITS_FIELDS, "limits", (h) =>
        native.runtimeConfigSetLimits(handle, h),
      );
    }
    if (config.guarantees != null) {
      nested(config.guarantees, native.guaranteeSetNew, GUARANTEE_FIELDS, "guarantees", (h) =>
        native.runtimeConfigSetGuarantees(handle, h),
      );
    }
    if (config.reconnect != null) {
      const make =
        config.reconnect.never === true ? native.reconnectPolicyNever : native.reconnectPolicyNew;
      nested(
        config.reconnect,
        make,
        RECONNECT_FIELDS,
        "reconnect",
        (h) => native.runtimeConfigSetReconnect(handle, h),
        ["never"],
      );
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

  /** Duplicates this runtime's connections discarded inside a negotiated
   * deduplication window, a BigInt; 0n without deduplication. */
  suppressedDuplicates() {
    return native.runtimeSuppressedDuplicates(this[HANDLE]);
  }

  /** Outgoing transfers this runtime reset because their deadline passed,
   * a BigInt. */
  expiredTransfers() {
    return native.runtimeExpiredTransfers(this[HANDLE]);
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

  /** Stops admitting work, gives every finished transfer whose receipt
   * nobody holds until `deadlineMs` to reach the peer's transport, then
   * closes as `shutdown` does within what is left of it. Resolves to a
   * frozen `{ delivered, outstanding }` of BigInts; a non-zero
   * `outstanding` is not an error. Aborting cancels the drain where it is.
   * Consumes the runtime. */
  async drain(deadlineMs, { signal } = {}) {
    const drained = await op(
      this,
      () => native.runtimeDrain(consume(this), deadlineMs),
      native.runtimeDrainOpWait,
      native.runtimeDrainOpFd,
      signal,
    );
    return Object.freeze(drained);
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

  /** Serves QUIC on a socket that is already bound: its descriptor number
   * (on Windows its SOCKET), e.g. a predecessor's `Handoff.takeSocket()`
   * passed on through `child_process` `stdio`. The descriptor is libweida's
   * once the call reaches it; weida neither binds it nor sets options on
   * it. */
  async bindQuicSocket(socket, serverTls, { signal } = {}) {
    const tls = handleOf(serverTls, ServerTls);
    const binding = await op(
      this,
      () => native.listenerBindQuicSocket(this[HANDLE], socket, tls),
      native.listenerBindQuicSocketOpWait,
      native.listenerBindQuicSocketOpFd,
      signal,
    );
    return adopt(Binding, binding);
  }

  /** Serves this listener's endpoints through the relay at `relay`
   * (`weida://[relay-key@]host:port/.weida/relay`), registered under the
   * key `via` presents; diallers reach them as
   * `weida+relay://<key>@[relay-key@]host:port/path`. Rejects with
   * WEIDA_EREJECTED where the relay refused the registration. */
  async bindRelay(relay, via, serverTls, { signal } = {}) {
    const client = handleOf(via, ClientTls);
    const tls = handleOf(serverTls, ServerTls);
    const binding = await op(
      this,
      () => native.listenerBindRelay(this[HANDLE], relay, client, tls),
      native.listenerBindRelayOpWait,
      native.listenerBindRelayOpFd,
      signal,
    );
    return adopt(Binding, binding);
  }

  /** Serves a relay on this listener's bindings: registrations and tunnels
   * arrive at `Relay.accept()`. Throws WEIDA_EALREADY_REGISTERED on a
   * second call. */
  relay() {
    return adopt(Relay, native.listenerRelay(this[HANDLE]));
  }

  /** Answers on the in-process bus `bus`. */
  bindInproc(bus) {
    return adopt(LocalBinding, native.listenerBindInproc(this[HANDLE], bus));
  }

  /** Serves this listener's endpoints on the `AF_UNIX` socket `path`, reached
   * at `weida+unix://<encodeURIComponent(path)>/<endpoint>`. No TLS: the kernel
   * proves the peer. `path` MUST be in a directory this process owns and no
   * other user may write. Throws WEIDA_EUNSUPPORTED on Windows. */
  bindUnix(path) {
    return adopt(UnixBinding, native.listenerBindUnix(this[HANDLE], path));
  }

  /** Serves this listener's endpoints on the named pipe `\\.\pipe\<name>`,
   * owner-only and local-only, reached at `weida+pipe://<name>/<endpoint>`.
   * Throws WEIDA_EUNSUPPORTED off Windows. */
  bindPipe(name) {
    return adopt(PipeBinding, native.listenerBindPipe(this[HANDLE], name));
  }

  /** The replier serving `path` on every binding of this listener. */
  replier(path) {
    return adopt(Replier, native.listenerReplier(this[HANDLE], path));
  }
}

/** A QUIC binding: the socket a listener serves on, directly or through a
 * relay. Dropping it releases the handle only: the binding serves until
 * `close()`, a hand-over or the runtime's shutdown. */
export class Binding extends Handle {
  constructor() {
    super();
    noConstructor(
      "Binding",
      "Listener.bindQuic(), Listener.bindQuicSocket() or Listener.bindRelay()",
    );
  }

  localAddr() {
    return native.bindingLocalAddr(this[HANDLE]);
  }

  /** Stops accepting and closes the binding's connections with SHUTDOWN;
   * resolves once the port is free to bind again. A relayed binding ends
   * its registration at the relay. */
  async close({ signal } = {}) {
    await op(
      this,
      () => native.bindingClose(this[HANDLE]),
      native.bindingCloseOpWait,
      native.bindingCloseOpFd,
      signal,
    );
  }

  /** Hands the socket over to a successor process: every connection is
   * closed with RESTARTING, its close packet held in the `Handoff` and not
   * sent. Consumes the binding. Rejects with WEIDA_EUNSUPPORTED for a
   * relayed binding and WEIDA_ECLOSED for a closed one. */
  async handOver({ signal } = {}) {
    const handoff = await op(
      this,
      () => native.bindingHandOver(consume(this)),
      native.bindingHandOverOpWait,
      native.bindingHandOverOpFd,
      signal,
    );
    return adopt(Handoff, handoff);
  }

  /** Sends the close packets a predecessor held (`Handoff.bytes()`), so its
   * clients redial into this binding; call it once their endpoints are
   * registered. Resolves with how many were sent. The bytes need the
   * integrity of the channel that carries the socket. */
  async takeOver(held, { signal } = {}) {
    return await op(
      this,
      () => native.bindingTakeOver(this[HANDLE], held),
      native.bindingTakeOverOpWait,
      native.bindingTakeOverOpFd,
      signal,
    );
  }

  /** Closes every connection from the peer with this key (`sha256:` text)
   * with REJECTED; how many it closed. Not a ban: the peer redials under
   * its own policy. */
  disconnect(fingerprint) {
    return native.bindingDisconnect(this[HANDLE], fingerprint);
  }

  /** A new reader of a relayed binding's registration events, or null for
   * a binding that is not relayed. */
  relayEvents() {
    const handle = native.bindingRelayEvents(this[HANDLE]);
    return handle === null ? null : adopt(PeerEvents, handle);
  }
}

/** What `Binding.handOver` leaves for a successor: the socket and the close
 * packets held for its clients. Dropping it closes a socket not taken. */
export class Handoff extends Handle {
  constructor() {
    super();
    noConstructor("Handoff", "Binding.handOver()");
  }

  /** How many close packets are held. */
  held() {
    return native.handoffHeld(this[HANDLE]);
  }

  /** The address the socket is bound to. */
  localAddr() {
    return native.handoffLocalAddr(this[HANDLE]);
  }

  /** A copy of the held packets, for the successor's `Binding.takeOver`. */
  bytes() {
    return native.handoffBytes(this[HANDLE]);
  }

  /** The socket's descriptor (on Windows its SOCKET), the caller's from
   * then on; once, then WEIDA_ECLOSED. */
  takeSocket() {
    return native.handoffTakeSocket(this[HANDLE]);
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

/** An `AF_UNIX` binding; dropping it ends the binding and removes the socket
 * file. */
export class UnixBinding extends Handle {
  constructor() {
    super();
    noConstructor("UnixBinding", "Listener.bindUnix()");
  }

  /** The socket path bound. */
  path() {
    return native.unixBindingPath(this[HANDLE]);
  }
}

/** A named-pipe binding; dropping it ends the binding and releases the
 * name. */
export class PipeBinding extends Handle {
  constructor() {
    super();
    noConstructor("PipeBinding", "Listener.bindPipe()");
  }

  /** The pipe name bound, without `\\.\pipe\`. */
  name() {
    return native.pipeBindingName(this[HANDLE]);
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

  /** `request` with metadata (`{ contentType?, contentLen?, trace?, topic?,
   * achieved? }`, or null), copied when the request starts. */
  async requestWith(meta, body, { signal } = {}) {
    const data = bytes(body);
    const transfer = await op(
      this,
      () => withTransferMeta(meta, (m) => native.requesterRequestWith(this[HANDLE], m, data)),
      native.requesterRequestOpWait,
      native.requesterRequestOpFd,
      signal,
    );
    return adopt(IncomingTransfer, transfer);
  }

  /** Opens one exchange with `meta` (a `TransferMeta` object, or null),
   * copied when it starts: `body`, the request written like a socket and
   * ended with `finish()`, and `reply`, the stream the reply is awaited on.
   * The halves are independent, so the reply can be read while the body is
   * still open. Aborting abandons the exchange. */
  async open(meta, { signal } = {}) {
    const opened = await op(
      this,
      () => withTransferMeta(meta, (m) => native.requesterOpen(this[HANDLE], m)),
      native.requesterOpenOpWait,
      native.requesterOpenOpFd,
      signal,
    );
    return Object.freeze({
      body: adopt(OutgoingTransfer, opened.body),
      reply: adopt(ReplyStream, opened.reply),
    });
  }

  /** A frozen array with one frozen record per live connection, in dial
   * order: `{ url, ageUs, redials, transport, remote }`, every number a
   * BigInt, `transport` ({ path, tx, rx }) and `remote` null when absent. */
  connectionStats() {
    return Object.freeze(native.requesterConnectionStats(this[HANDLE]).map(freezeStats));
  }

  /** A new reader of this requester's peer events, from now on. */
  events() {
    return adopt(PeerEvents, native.requesterEvents(this[HANDLE]));
  }
}

/** The reply half of an exchange `Requester.open` started. Dropping it
 * before `recv` tells the replier nobody is listening. */
export class ReplyStream extends Handle {
  constructor() {
    super();
    noConstructor("ReplyStream", "Requester.open()");
  }

  /** The reply, as soon as its header arrives; a refused request rejects
   * with the refusal's WeidaError. Consumes the stream. */
  async recv({ signal } = {}) {
    const transfer = await op(
      this,
      () => native.replyStreamRecv(consume(this)),
      native.replyStreamRecvOpWait,
      native.replyStreamRecvOpFd,
      signal,
    );
    return adopt(IncomingTransfer, transfer);
  }
}

/** The peer events of one requester, or of a relayed binding's
 * registration, read like a queue or iterated with `for await`; the loop
 * ends once the requester or the registration is gone, and leaving it
 * early drops the reader. */
export class PeerEvents extends Handle {
  constructor() {
    super();
    noConstructor("PeerEvents", "Requester.events() or Binding.relayEvents()");
  }

  /** The next event as a frozen object, or null once the requester, or the
   * relayed binding's registration, is gone. */
  async recv({ signal } = {}) {
    const handle = this[HANDLE];
    const event = await call(
      this,
      () => native.peerEventsRecv(handle),
      () => native.peerEventsFd(handle),
      signal,
    );
    return event === null ? null : Object.freeze(event);
  }

  async *[Symbol.asyncIterator]() {
    try {
      for (let event = await this.recv(); event !== null; event = await this.recv()) {
        yield event;
      }
    } finally {
      this.drop();
    }
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

/** The relay a listener serves on `/.weida/relay`. Dropping it closes every
 * registration's connection, so the hidden peers redial. */
export class Relay extends Handle {
  constructor() {
    super();
    noConstructor("Relay", "Listener.relay()");
  }

  /** The next registration or tunnel asking to be served. An anonymous
   * registration and a tunnel to a key nobody registered are refused before
   * they get here. */
  async accept({ signal } = {}) {
    const handle = this[HANDLE];
    const request = await call(
      this,
      () => native.relayAccept(handle),
      () => native.relayFd(handle),
      signal,
    );
    return adopt(RelayRequest, request);
  }
}

/** A registration or a tunnel asking the relay to be served. Dropped
 * unanswered, it answers NO_REPLY. */
export class RelayRequest extends Handle {
  constructor() {
    super();
    noConstructor("RelayRequest", "Relay.accept()");
  }

  /** The key (`sha256:` text) the asker proved, null for an anonymous
   * dialler; always a key for a registration. */
  peerFingerprint() {
    return native.relayRequestPeerFingerprint(this[HANDLE]);
  }

  /** null for a registration; for a tunnel, the key of the peer it reaches. */
  target() {
    return native.relayRequestTarget(this[HANDLE]);
  }

  /** Serves it. A registration replaces one of the same key on another
   * connection. Consumes the request. */
  async admit({ signal } = {}) {
    await op(
      this,
      () => native.relayRequestAdmit(consume(this)),
      native.relayRequestAdmitOpWait,
      native.relayRequestAdmitOpFd,
      signal,
    );
  }

  /** Refuses it: the asker fails with WEIDA_EREJECTED. Consumes the
   * request. */
  async refuse({ signal } = {}) {
    await op(
      this,
      () => native.relayRequestRefuse(consume(this)),
      native.relayRequestRefuseOpWait,
      native.relayRequestRefuseOpFd,
      signal,
    );
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

  /** The number of the connection this request arrived on, a BigInt. */
  connection() {
    return native.incomingRequestConnection(this[HANDLE]);
  }

  /** Detaches the body, to be read like a socket; once. */
  takeBody() {
    return adopt(IncomingTransfer, native.incomingRequestTakeBody(this[HANDLE]));
  }

  /** Opens the reply stream with `meta` (a `TransferMeta` object, or null).
   * Consumes the request. */
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

  /** Refuses the exchange with `code`, a `WEIDA_ERROR_CODE_*` name, instead of
   * a reply: the requester's request rejects with the matching WeidaError.
   * Consumes the request; an unknown name is a TypeError and consumes
   * nothing. */
  async refuse(code, { signal } = {}) {
    const value = constant("WEIDA_ERROR_CODE_", code);
    await op(
      this,
      () => native.incomingRequestRefuse(consume(this), value),
      native.incomingRequestRefuseOpWait,
      native.incomingRequestRefuseOpFd,
      signal,
    );
  }

  /** Resolves when the requester stops wanting a reply, a finished reply was
   * read whole, or the connection is gone. Started at the call, so take it
   * before `reply` or `refuse`; it outlives the request. A pending one keeps
   * the process alive: abort it through `signal` once it no longer matters. */
  canceled({ signal } = {}) {
    return op(
      { [WAITS]: new Set() },
      () => native.incomingRequestCanceled(this[HANDLE]),
      native.incomingRequestCanceledOpWait,
      native.incomingRequestCanceledOpFd,
      signal,
    );
  }
}

/** An inbound transfer: a payload read like a socket. */
export class IncomingTransfer extends Handle {
  constructor() {
    super();
    noConstructor("IncomingTransfer", "Requester.request(), ReplyStream.recv() or IncomingRequest.takeBody()");
  }

  meta() {
    return freezeMeta(native.incomingTransferMeta(this[HANDLE]));
  }

  /** The number of the connection this transfer arrived on, a BigInt. */
  connection() {
    return native.incomingTransferConnection(this[HANDLE]);
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
    noConstructor("OutgoingTransfer", "IncomingRequest.reply() or Requester.open()");
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

  /** The trace context propagated with this transfer, as `traceparent`
   * text, or null; a reply inherits its request's. */
  trace() {
    return native.outgoingTransferTrace(this[HANDLE]);
  }

  /** Orders this transfer against the other streams of its connection:
   * higher is sent first, the default is 0. A no-op on local transports. */
  setPriority(priority) {
    native.outgoingTransferSetPriority(this[HANDLE], priority);
  }

  /** Resets the transfer with WEIDA_ECANCELED for the reader unless its
   * bytes are all acknowledged within `ms`; a write or finish past it
   * throws WEIDA_EEXPIRED. */
  expireAfterMs(ms) {
    native.outgoingTransferExpireAfterMs(this[HANDLE], ms);
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
