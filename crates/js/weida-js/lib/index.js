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
  if (meta.gap != null) Object.freeze(meta.gap);
  Object.freeze(meta.report);
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
/** The keys besides the fields: the nested objects and the resolver. */
const EXTRA = [...NESTED, "resolver"];
const CONFIG_KEYS = [...CONFIG_FIELDS.map(([key]) => key), ...EXTRA];

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
  ["report", reportLevels, "value"],
  ["reportMode", native.transferMetaSetReportMode, "WEIDA_REPORT_MODE_"],
];

/** A terms object as a handle for one call: `make()` the handle, set
 * `fields` of `object`, `use` it and drop it; null `object` is `use(null)`. */
function withFields(object, make, fields, what, use) {
  if (object == null) {
    return use(null);
  }
  const handle = make();
  try {
    applyFields(handle, object, fields, what);
    return use(handle);
  } finally {
    native.drop(handle);
  }
}

/** A transfer meta `{ contentType?, contentLen?, trace?, topic?,
 * achieved?, report?, reportMode? }` as a handle for one call. */
function withTransferMeta(meta, use) {
  return withFields(meta, native.transferMetaNew, META_FIELDS, "TransferMeta", use);
}

const JOIN_FIELDS = [
  ["maxAgeMs", native.joinTermsSetMaxAgeMs, "value"],
  ["maxLayer", native.joinTermsSetMaxLayer, "value"],
];

const SEGMENT_FIELDS = [
  ["maxAgeMs", native.segmentTermsSetMaxAgeMs, "value"],
  ["priority", native.segmentTermsSetPriority, "value"],
  ["followsUpstream", native.segmentTermsSetFollowsUpstream, "value"],
];

const FETCH_FIELDS = [
  ["list", (h, list) => native.fetchOptionsSetList(h, handleOf(list, PieceList)), "value"],
  ["stallMs", native.fetchOptionsSetStallMs, "value"],
  ["minRate", native.fetchOptionsSetMinRate, "value"],
  ["perPeer", native.fetchOptionsSetPerPeer, "value"],
  ["maxInflight", native.fetchOptionsSetMaxInflight, "value"],
  ["maxPeers", native.fetchOptionsSetMaxPeers, "value"],
  ["haveRefreshMs", native.fetchOptionsSetHaveRefreshMs, "value"],
];

/** How many layers a segment carries: 0 to 15. */
export const MAX_SEGMENT_LAYERS = native.CONSTANTS.WEIDA_MAX_SEGMENT_LAYERS;

/** The first cursor level an application may name; the levels below it are
 * weida's own, named "WEIDA_ACKNOWLEDGEMENT_*". */
export const CURSOR_LEVEL_APPLICATION_FLOOR =
  native.CONSTANTS.WEIDA_CURSOR_LEVEL_APPLICATION_FLOOR;

/** The `datagramReceiveBytes` a profile with flows uses. */
export const DEFAULT_DATAGRAM_RECEIVE_BYTES =
  native.CONSTANTS.WEIDA_DEFAULT_DATAGRAM_RECEIVE_BYTES;

/** The bytes of a blob id: a BLAKE3 hash. */
export const BLOB_ID_LEN = native.CONSTANTS.WEIDA_BLOB_ID_LEN;

/** The most pieces a blob has. */
export const MAX_PIECES = native.CONSTANTS.WEIDA_MAX_PIECES;

/** The smallest piece size as its base-2 logarithm: 1 KiB. */
export const PIECE_SIZE_MIN_LOG2 = native.CONSTANTS.WEIDA_PIECE_SIZE_MIN_LOG2;

/** The largest piece size as its base-2 logarithm: 16 MiB. */
export const PIECE_SIZE_MAX_LOG2 = native.CONSTANTS.WEIDA_PIECE_SIZE_MAX_LOG2;

/** The default piece size as its base-2 logarithm: 1 MiB. */
export const PIECE_SIZE_DEFAULT_LOG2 = native.CONSTANTS.WEIDA_PIECE_SIZE_DEFAULT_LOG2;

/** The id of `data` (a string is hashed as UTF-8): its BLAKE3 hash,
 * `BLOB_ID_LEN` bytes. */
export function blobIdOf(data) {
  return native.blobIdOf(bytes(data));
}

const FLOW_META_FIELDS = [
  ["contentType", native.flowMetaSetContentType, "value"],
  ["trace", native.flowMetaSetTrace, "value"],
  ["topic", native.flowMetaSetTopic, "value"],
];

/** A cursor level in: a "WEIDA_ACKNOWLEDGEMENT_*" name becomes its value,
 * an application stage (a number or a BigInt) passes on; C refuses 6 to
 * 15. */
function cursorLevel(level) {
  return typeof level === "string" ? constant("WEIDA_ACKNOWLEDGEMENT_", level) : level;
}

/** `TransferMeta.report`: an array of cursor levels. */
function reportLevels(handle, levels) {
  if (!Array.isArray(levels)) {
    throw new TypeError("expected an array of cursor levels");
  }
  native.transferMetaSetReport(handle, levels.map(cursorLevel));
}

const FILES_FIELDS = [
  ["names", native.filesOptionsSetNames, "value"],
  ["pollMs", native.filesOptionsSetPollMs, "value"],
];

/** The `WEIDA_ALPN_*` value of `alpn`, or null when it is not given. */
function tlsAlpn(alpn) {
  return alpn == null ? null : constant("WEIDA_ALPN_", alpn);
}

/** Checks that `names` is an array of strings. */
function checkNames(names) {
  if (!Array.isArray(names) || !names.every((n) => typeof n === "string")) {
    throw new TypeError("expected an array of names");
  }
  return names;
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

function configHandle(config, resolver) {
  if (config == null) {
    return null;
  }
  const handle = native.runtimeConfigNew();
  try {
    applyFields(handle, config, CONFIG_FIELDS, "RuntimeConfig", EXTRA);
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
    if (resolver !== null) {
      native.runtimeConfigSetResolver(handle, resolver);
    }
  } catch (error) {
    native.drop(handle);
    throw error;
  }
  return handle;
}

/** Answers one lookup through `resolve`; never rejects. A throw, a
 * rejection or an answer that is not an array of strings refuses it. */
async function answerLookup(handle, resolve) {
  try {
    const lookup = Object.freeze({
      name: native.resolvedName(handle),
      port: native.resolvedPort(handle),
      maxAddresses: native.resolvedMaxAddresses(handle),
    });
    let addresses;
    try {
      addresses = await resolve(lookup);
    } catch (error) {
      native.resolvedRefuse(handle, error instanceof Error ? error.message : String(error));
      return;
    }
    if (!Array.isArray(addresses) || !addresses.every((a) => typeof a === "string")) {
      native.resolvedRefuse(handle, "the resolver did not answer an array of address strings");
      return;
    }
    native.resolvedAnswer(handle, addresses);
  } catch {
    // A spent lookup drops as a no-op; an unspent one fails its dial as
    // dropped unanswered.
    native.drop(handle);
  }
}

/** Reads `resolver`'s lookups until no configuration or runtime holds it,
 * answering them concurrently. Its watch is weak: it never keeps the
 * process alive. */
async function serveLookups(resolver, resolve) {
  const owner = { [WAITS]: new Set() };
  try {
    for (;;) {
      const handle = await call(
        owner,
        () => native.resolverRecv(resolver),
        () => native.resolverFd(resolver),
        undefined,
        true,
      );
      if (handle === null) {
        break;
      }
      void answerLookup(handle, resolve);
    }
  } catch {
    // A descriptor that cannot be made ends the service; the lookups after
    // it fail their dial with WEIDA_EINVALID_ADDRESS.
  } finally {
    native.drop(resolver);
  }
}

/** A weida runtime: the reactor it owns, and every endpoint made from it. */
export class Runtime extends Handle {
  constructor() {
    super();
    noConstructor("Runtime", "Runtime.owned()");
  }

  /** A runtime on a reactor of its own.
   *
   * A `resolver` in `config` answers each lookup of this runtime's dials on
   * the JS thread: it is called with a frozen `{ name, port, maxAddresses }`
   * and answers `"ip:port"` strings in dial order; a throw refuses the
   * lookup, and the dial rejects with `WEIDA_EINVALID_ADDRESS` carrying the
   * message. It is served until the runtime and everything made from it are
   * gone, and never keeps the process alive. */
  static owned(config) {
    const resolve = config?.resolver ?? null;
    if (resolve !== null && typeof resolve !== "function") {
      throw new TypeError("resolver is a function (lookup) => addresses");
    }
    const resolver = resolve === null ? null : native.resolverNew();
    let cfg = null;
    try {
      cfg = configHandle(config, resolver);
      const runtime = adopt(Runtime, native.runtimeOwned(cfg));
      if (resolver !== null) {
        void serveLookups(resolver, resolve);
      }
      return runtime;
    } catch (error) {
      if (resolver !== null) {
        native.drop(resolver);
      }
      throw error;
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

  pusher(clientTls) {
    return adopt(Pusher, native.runtimePusher(this[HANDLE], handleOf(clientTls, ClientTls)));
  }

  /** A subscriber dialling on `clientTls`. Two subscribers on one runtime
   * share its pooled connection and collide on the path
   * (WEIDA_EALREADY_REGISTERED): use one runtime per subscriber of a path. */
  subscriber(clientTls) {
    return adopt(Subscriber, native.runtimeSubscriber(this[HANDLE], handleOf(clientTls, ClientTls)));
  }

  /** A dialling pair on `clientTls`: one peer, dialled with `connect`. */
  pair(clientTls) {
    return adopt(Paired, native.runtimePair(this[HANDLE], handleOf(clientTls, ClientTls)));
  }

  /** A surveyor dialling respondents on `clientTls`. */
  surveyor(clientTls) {
    return adopt(Surveyor, native.runtimeSurveyor(this[HANDLE], handleOf(clientTls, ClientTls)));
  }

  /** A dish dialling radios on `clientTls`. Two dishes on one runtime
   * share its pooled connection and collide on the path
   * (WEIDA_EALREADY_REGISTERED): use one runtime per dish. */
  dish(clientTls) {
    return adopt(Dish, native.runtimeDish(this[HANDLE], handleOf(clientTls, ClientTls)));
  }

  /** A raw peer dialling on `clientTls`, which opens datagram flows. */
  peer(clientTls) {
    return adopt(Peer, native.runtimePeer(this[HANDLE], handleOf(clientTls, ClientTls)));
  }

  /** A swarm dialling seeders on `clientTls`: it fetches one blob from
   * every seeder it connected at once. */
  swarm(clientTls) {
    return adopt(Swarm, native.runtimeSwarm(this[HANDLE], handleOf(clientTls, ClientTls)));
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

  /** Whether the trust holds no pin and no anchor, so it accepts only what
   * each dialled address names. */
  isEmpty() {
    return native.trustIsEmpty(this[HANDLE]);
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
    return adopt(Identity, native.identityGenerateFor(checkNames(names)));
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

/** The TLS terms a requester dials on, under `trust` (a `Trust`, copied, or
 * a `TrustSource`, shared): anonymous, or presenting `identity` (an
 * `Identity`, copied, or an `IdentitySource`, shared). `alpn` is the
 * `WEIDA_ALPN_*` token to ask for, `WEIDA_ALPN_WEIDA` unless given. */
export class ClientTls extends Handle {
  constructor(trust, { identity, alpn } = {}) {
    super();
    let make;
    if (trust instanceof TrustSource) {
      make = () => native.clientTlsNewSource(trust[HANDLE]);
    } else if (trust instanceof Trust) {
      make = () => native.clientTlsNew(trust[HANDLE]);
    } else {
      throw new TypeError("expected a Trust or a TrustSource");
    }
    if (!(identity == null || identity instanceof Identity || identity instanceof IdentitySource)) {
      throw new TypeError("identity is an Identity or an IdentitySource");
    }
    const token = tlsAlpn(alpn);
    const handle = make();
    try {
      if (identity instanceof IdentitySource) {
        native.clientTlsSetIdentitySource(handle, identity[HANDLE]);
      } else if (identity != null) {
        native.clientTlsSetIdentity(handle, identity[HANDLE]);
      }
      if (token !== null) {
        native.clientTlsSetAlpn(handle, token);
      }
    } catch (error) {
      native.drop(handle);
      throw error;
    }
    init(this, handle);
  }
}

/** The TLS terms a QUIC binding serves on, presenting `identity` (an
 * `Identity`, copied, or an `IdentitySource`, shared, whose renewals reach
 * the next peer with no re-bind). With `requireClient` every client must
 * present a key: one the `Trust` or `TrustSource` accepts, or with
 * "any-key" any key it proves it holds. `alpn` is the `WEIDA_ALPN_*` token
 * to offer, `WEIDA_ALPN_WEIDA` unless given. */
export class ServerTls extends Handle {
  constructor(identity, { requireClient, alpn } = {}) {
    super();
    let make;
    if (identity instanceof IdentitySource) {
      make = () => native.serverTlsNewSource(identity[HANDLE]);
    } else if (identity instanceof Identity) {
      make = () => native.serverTlsNew(identity[HANDLE]);
    } else {
      throw new TypeError("expected an Identity or an IdentitySource");
    }
    if (
      !(
        requireClient == null ||
        requireClient === "any-key" ||
        requireClient instanceof Trust ||
        requireClient instanceof TrustSource
      )
    ) {
      throw new TypeError('requireClient is a Trust, a TrustSource or "any-key"');
    }
    const token = tlsAlpn(alpn);
    const handle = make();
    try {
      if (requireClient === "any-key") {
        native.serverTlsRequireAnyKey(handle);
      } else if (requireClient instanceof TrustSource) {
        native.serverTlsRequireClientSource(handle, requireClient[HANDLE]);
      } else if (requireClient != null) {
        native.serverTlsRequireClient(handle, requireClient[HANDLE]);
      }
      if (token !== null) {
        native.serverTlsSetAlpn(handle, token);
      }
    } catch (error) {
      native.drop(handle);
      throw error;
    }
    init(this, handle);
  }
}

/** Where an identity comes from: a value, an ephemeral identity, one driven
 * from outside, or a directory an agent rewrites. A source is shared by the
 * TLS terms made from it, so an update reaches their next handshake;
 * dropping this handle leaves the source alive wherever it is held. */
export class IdentitySource extends Handle {
  constructor() {
    super();
    noConstructor(
      "IdentitySource",
      "IdentitySource.fromIdentity(), .ephemeral(), .external() or .files()",
    );
  }

  /** A source that holds `identity` and never changes; throws WEIDA_ETLS
   * when its material does not read. */
  static fromIdentity(identity) {
    return adopt(IdentitySource, native.identitySourceFromIdentity(handleOf(identity, Identity)));
  }

  /** A self-signed identity generated in memory for `names`. */
  static ephemeral(names = []) {
    return adopt(IdentitySource, native.identitySourceEphemeral(checkNames(names)));
  }

  /** A source that starts as `identity` and changes on `update()`. */
  static external(identity) {
    return adopt(IdentitySource, native.identitySourceExternal(handleOf(identity, Identity)));
  }

  /** The directory `dir` holding `cert.pem` and `key.pem`, bootstrapped
   * with a self-signed identity for `names` when empty and reloaded when
   * the files change, checked at the next use after `pollMs`. */
  static files(dir, options = {}) {
    let handle;
    nested(options ?? {}, native.filesOptionsNew, FILES_FIELDS, "FilesOptions", (h) => {
      handle = native.identitySourceFiles(dir, h);
    });
    return adopt(IdentitySource, handle);
  }

  /** The identity of the moment. */
  current() {
    return adopt(Identity, native.identitySourceCurrent(this[HANDLE]));
  }

  /** The current key's fingerprint, `sha256:` text. */
  fingerprint() {
    return native.identitySourceFingerprint(this[HANDLE]);
  }

  /** Replaces the identity: RENEWED when the key is unchanged, KEY_CHANGED
   * when it is not. */
  update(identity) {
    native.identitySourceUpdate(this[HANDLE], handleOf(identity, Identity));
  }

  /** Reports a renewal that failed without material (RENEWAL_FAILED). */
  reportFailure(reason) {
    native.identitySourceReportFailure(this[HANDLE], reason);
  }

  /** Reports that the hand-off credential had already been redeemed. */
  reportHandoffStolen() {
    native.identitySourceReportHandoffStolen(this[HANDLE]);
  }

  /** Reloads a file source now: true when the files changed and were
   * loaded, false when nothing changed or this is not a file source. */
  reload() {
    return native.identitySourceReload(this[HANDLE]);
  }

  /** A reader of this source's events from now on. */
  events() {
    return adopt(IdentityEvents, native.identitySourceEvents(this[HANDLE]));
  }

  /** A handle that does not keep the source alive. */
  downgrade() {
    return adopt(WeakIdentitySource, native.identitySourceDowngrade(this[HANDLE]));
  }
}

/** A handle to an `IdentitySource` that does not keep it alive. */
export class WeakIdentitySource extends Handle {
  constructor() {
    super();
    noConstructor("WeakIdentitySource", "IdentitySource.downgrade()");
  }

  /** The source while anything holds it, else null. */
  upgrade() {
    const handle = native.weakIdentitySourceUpgrade(this[HANDLE]);
    return handle === null ? null : adopt(IdentitySource, handle);
  }
}

/** Whom a peer is accepted as, as a source: a `Trust` that never changes,
 * or one driven from outside. Shared by the TLS terms made from it, so an
 * update reaches their next handshake. */
export class TrustSource extends Handle {
  constructor() {
    super();
    noConstructor("TrustSource", "TrustSource.fromTrust() or .external()");
  }

  /** A source that holds a copy of `trust` and never changes. */
  static fromTrust(trust) {
    return adopt(TrustSource, native.trustSourceFromTrust(handleOf(trust, Trust)));
  }

  /** A source that starts as a copy of `trust` and changes on `update()`. */
  static external(trust) {
    return adopt(TrustSource, native.trustSourceExternal(handleOf(trust, Trust)));
  }

  /** A copy of the trust of the moment. */
  current() {
    return adopt(Trust, native.trustSourceCurrent(this[HANDLE]));
  }

  /** Replaces the trust (TRUST_REFRESHED). */
  update(trust) {
    native.trustSourceUpdate(this[HANDLE], handleOf(trust, Trust));
  }

  /** Reports a refresh that failed (RENEWAL_FAILED). */
  reportFailure(reason) {
    native.trustSourceReportFailure(this[HANDLE], reason);
  }

  /** Whether the trust of the moment holds no pin and no anchor. */
  isEmpty() {
    return native.trustSourceIsEmpty(this[HANDLE]);
  }

  /** A reader of this source's events from now on. */
  events() {
    return adopt(IdentityEvents, native.trustSourceEvents(this[HANDLE]));
  }

  /** A handle that does not keep the source alive. */
  downgrade() {
    return adopt(WeakTrustSource, native.trustSourceDowngrade(this[HANDLE]));
  }
}

/** A handle to a `TrustSource` that does not keep it alive. */
export class WeakTrustSource extends Handle {
  constructor() {
    super();
    noConstructor("WeakTrustSource", "TrustSource.downgrade()");
  }

  /** The source while anything holds it, else null. */
  upgrade() {
    const handle = native.weakTrustSourceUpgrade(this[HANDLE]);
    return handle === null ? null : adopt(TrustSource, handle);
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

  /** The puller serving `path` on every binding of this listener. */
  puller(path) {
    return adopt(Puller, native.listenerPuller(this[HANDLE], path));
  }

  /** The publisher serving `path` on every binding of this listener. */
  publisher(path) {
    return adopt(Publisher, native.listenerPublisher(this[HANDLE], path));
  }

  /** A bound pair serving `path` on every binding of this listener. */
  pair(path) {
    return adopt(Paired, native.listenerPair(this[HANDLE], path));
  }

  /** The respondent serving `path` on every binding of this listener. */
  respondent(path) {
    return adopt(Respondent, native.listenerRespondent(this[HANDLE], path));
  }

  /** A bus member accepting on `path` and dialling the other members on
   * `clientTls`. */
  bus(path, clientTls) {
    return adopt(BusMember, native.listenerBus(this[HANDLE], path, handleOf(clientTls, ClientTls)));
  }

  /** The radio serving `path` on every binding of this listener; dropping
   * it releases the path. */
  radio(path) {
    return adopt(Radio, native.listenerRadio(this[HANDLE], path));
  }

  /** A raw acceptor serving `path`: it takes both stream kinds and flows on
   * the path, tagged. */
  acceptor(path) {
    return adopt(Acceptor, native.listenerAcceptor(this[HANDLE], path));
  }

  /** A seeder serving `path`: it hands each fetch's session to the
   * application, which answers its orders from its own storage. */
  seeder(path) {
    return adopt(Seeder, native.listenerSeeder(this[HANDLE], path));
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

/** The events of one identity or trust source, read like a queue or
 * iterated with `for await`; the loop ends once the source is gone, and
 * leaving it early drops the reader. */
export class IdentityEvents extends Handle {
  constructor() {
    super();
    noConstructor("IdentityEvents", "IdentitySource.events() or TrustSource.events()");
  }

  /** The next event as a frozen object, or null once the source is gone. */
  async recv({ signal } = {}) {
    const handle = this[HANDLE];
    const event = await call(
      this,
      () => native.identityEventsRecv(handle),
      () => native.identityEventsFd(handle),
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

/** The cursor sets one transfer's receiver reports, read like a queue or
 * iterated with `for await`; each set is the newest offset per reported
 * level, changes since the last read coalesced. The loop ends once no more
 * cursors come, and leaving it early drops the reader. */
export class Cursors extends Handle {
  constructor() {
    super();
    noConstructor("Cursors", "OutgoingTransfer.cursors()");
  }

  /** The newest set after a change, a frozen array of frozen
   * `{ level, offset }` in ascending level order, or null once no more
   * cursors come (the reporter finished, the stream was reset or the
   * connection is gone). */
  async recv({ signal } = {}) {
    const handle = this[HANDLE];
    const set = await call(
      this,
      () => native.cursorsRecv(handle),
      () => native.cursorsFd(handle),
      signal,
    );
    return set === null ? null : Object.freeze(set.map((entry) => Object.freeze(entry)));
  }

  async *[Symbol.asyncIterator]() {
    try {
      for (let set = await this.recv(); set !== null; set = await this.recv()) {
        yield set;
      }
    } finally {
      this.drop();
    }
  }
}

/** The receiver's end of a report: how far each ordered level got. Dropping
 * it without `finish()` flushes nothing. */
export class Reporter extends Handle {
  constructor() {
    super();
    noConstructor("Reporter", "IncomingTransfer.reporter()");
  }

  /** The levels the sender ordered, ascending, frozen. */
  levels() {
    return Object.freeze(native.reporterLevels(this[HANDLE]));
  }

  /** The `WEIDA_REPORT_MODE_*` the sender asked for. */
  mode() {
    return native.reporterMode(this[HANDLE]);
  }

  /** Progress records are written when a level advanced `bytes` or
   * `intervalMs` passed, whichever comes first; 0 and 0 write every
   * report. No effect in final-only mode. */
  setGranularity(bytes, intervalMs) {
    native.reporterSetGranularity(this[HANDLE], bytes, intervalMs);
  }

  /** Reports that `level` has reached `offset`; a level the sender did not
   * order is ignored. Never rejects for a transport reason. */
  report(level, offset, { signal } = {}) {
    return op(
      this,
      () => native.reporterReport(this[HANDLE], cursorLevel(level), offset),
      native.reporterReportOpWait,
      native.reporterReportOpFd,
      signal,
    );
  }

  /** Flushes the newest offset per reported level and ends the report.
   * Consumes the reporter. */
  finish({ signal } = {}) {
    return op(
      this,
      () => native.reporterFinish(consume(this)),
      native.reporterFinishOpWait,
      native.reporterFinishOpFd,
      signal,
    );
  }
}

/** The dialling side of the stream core: it opens one-way streams,
 * exchanges, segments and datagram flows to the peers it dialled,
 * round-robin. */
export class Peer extends Handle {
  constructor() {
    super();
    noConstructor("Peer", "Runtime.peer()");
  }

  /** Dials `url`; the runtime redials it when the connection is lost. */
  async connect(url, { signal } = {}) {
    await op(
      this,
      () => native.peerConnect(this[HANDLE], url),
      native.peerConnectOpWait,
      native.peerConnectOpFd,
      signal,
    );
  }

  /** Forgets a dialled address: whether it was dialled. */
  disconnect(url) {
    return native.peerDisconnect(this[HANDLE], url);
  }

  /** The number of connected peers. */
  peerCount() {
    return native.peerPeerCount(this[HANDLE]);
  }

  /** A frozen array with one frozen record per live connection, as
   * `Requester.connectionStats`. */
  connectionStats() {
    return Object.freeze(native.peerConnectionStats(this[HANDLE]).map(freezeStats));
  }

  /** A new reader of this peer's events, from now on. */
  events() {
    return adopt(PeerEvents, native.peerEvents(this[HANDLE]));
  }

  /** The bodies the outbox discarded, a BigInt. */
  dropped() {
    return native.peerDropped(this[HANDLE]);
  }

  /** Opens a one-way stream to the next peer with `meta` (a `TransferMeta`
   * object, or null), written like a socket and ended with `finish()`. With
   * every peer down it waits for the redial, bounded by `sendTimeout`;
   * a peer never connected rejects with WEIDA_ENOT_CONNECTED. Aborting
   * abandons it. */
  async open(meta, { signal } = {}) {
    const transfer = await op(
      this,
      () => withTransferMeta(meta, (m) => native.peerOpen(this[HANDLE], m)),
      native.peerOpenOpWait,
      native.peerOpenOpFd,
      signal,
    );
    return adopt(OutgoingTransfer, transfer);
  }

  /** Opens one exchange with the next peer, as `Requester.open`: a frozen
   * `{ body, reply }`. */
  async openBi(meta, { signal } = {}) {
    const opened = await op(
      this,
      () => withTransferMeta(meta, (m) => native.peerOpenBi(this[HANDLE], m)),
      native.peerOpenBiOpWait,
      native.peerOpenBiOpFd,
      signal,
    );
    return Object.freeze({
      body: adopt(OutgoingTransfer, opened.body),
      reply: adopt(ReplyStream, opened.reply),
    });
  }

  /** Sends `body` (a string is sent as UTF-8) as one message with `meta` (a
   * `TransferMeta` object, or null); resolves once it is written or held in
   * the outbox. */
  async send(meta, body, { signal } = {}) {
    const data = bytes(body);
    await op(
      this,
      () => withTransferMeta(meta, (m) => native.peerSend(this[HANDLE], m, data)),
      native.peerSendOpWait,
      native.peerSendOpFd,
      signal,
    );
  }

  /** Opens segment n+1 on `topic` to the next peer, on `terms`
   * `{ maxAgeMs?, priority?, followsUpstream? }`: it supersedes this peer's
   * copy of segment n there, as a radio's does. */
  async segment(topic, terms = null, { signal } = {}) {
    const segment = await op(
      this,
      () =>
        withFields(terms, native.segmentTermsNew, SEGMENT_FIELDS, "SegmentTerms", (t) =>
          native.peerSegment(this[HANDLE], topic, t),
        ),
      native.peerSegmentOpWait,
      native.peerSegmentOpFd,
      signal,
    );
    return adopt(Segment, segment);
  }

  /** What this peer's segments lost on `topic`, a frozen record as
   * `Publisher.droppedOn`, or null when nothing was. */
  segmentDrops(topic) {
    const [record] = native.peerSegmentDrops(this[HANDLE], topic);
    return record === undefined ? null : Object.freeze(record);
  }

  /** Registers a datagram flow to the next peer with `meta` (a
   * `{ contentType?, trace?, topic? }` object, or null), resolving once its
   * header is written. A refusal arrives later, as a `send` that throws.
   * Rejects with WEIDA_EDATAGRAMS_UNAVAILABLE unless both runtimes carry
   * datagrams (`limits.datagramReceiveBytes` above 0). */
  async openFlow(meta = null, { signal } = {}) {
    const flow = await op(
      this,
      () =>
        withFields(meta, native.flowMetaNew, FLOW_META_FIELDS, "FlowMeta", (m) =>
          native.peerOpenFlow(this[HANDLE], m),
        ),
      native.peerOpenFlowOpWait,
      native.peerOpenFlowOpFd,
      signal,
    );
    return adopt(Flow, flow);
  }
}

/** The bound side of the stream core, serving one endpoint path. */
export class Acceptor extends Handle {
  constructor() {
    super();
    noConstructor("Acceptor", "Listener.acceptor()");
  }

  path() {
    return native.acceptorPath(this[HANDLE]);
  }

  /** The next thing that arrived on the path, a frozen object by `kind`:
   * `{ kind: "WEIDA_INCOMING_STREAM", transfer }`,
   * `{ kind: "WEIDA_INCOMING_EXCHANGE", request }`,
   * `{ kind: "WEIDA_INCOMING_SUBSCRIBED", consumer }`,
   * `{ kind: "WEIDA_INCOMING_CREDIT", id, filter, limit }` (BigInt id and
   * limit), `{ kind: "WEIDA_INCOMING_UNSUBSCRIBED", id, filter }` (`filter`
   * null when the whole connection went away), or
   * `{ kind: "WEIDA_INCOMING_FLOW", flow }`. */
  async accept({ signal } = {}) {
    const handle = this[HANDLE];
    const r = await call(
      this,
      () => native.acceptorAccept(handle),
      () => native.acceptorFd(handle),
      signal,
    );
    switch (r.kind) {
      case "WEIDA_INCOMING_STREAM":
        return Object.freeze({ kind: r.kind, transfer: adopt(IncomingTransfer, r.transfer) });
      case "WEIDA_INCOMING_EXCHANGE":
        return Object.freeze({ kind: r.kind, request: adopt(IncomingRequest, r.request) });
      case "WEIDA_INCOMING_SUBSCRIBED":
        return Object.freeze({ kind: r.kind, consumer: adopt(Consumer, r.consumer) });
      case "WEIDA_INCOMING_FLOW":
        return Object.freeze({ kind: r.kind, flow: adopt(IncomingFlow, r.flow) });
      default:
        return Object.freeze(r);
    }
  }
}

/** One consumer that subscribed on an acceptor's path, and the way to
 * deliver to it. A queue honours its credit; nothing is delivered by
 * itself. */
export class Consumer extends Handle {
  constructor() {
    super();
    noConstructor("Consumer", "Acceptor.accept()");
  }

  /** The number of the connection it subscribed on, a BigInt shared by
   * every subscription of that connection and never reused. */
  id() {
    return native.consumerId(this[HANDLE]);
  }

  /** The filter the subscription was created with. */
  filter() {
    return native.consumerFilter(this[HANDLE]);
  }

  /** The endpoint path it subscribed on. */
  path() {
    return native.consumerPath(this[HANDLE]);
  }

  /** Opens one delivery with `meta` (a `TransferMeta` object, or null; the
   * topic the consumer filters on travels in it), addressed to the
   * subscribed path and written like a socket. Rejects with
   * WEIDA_ECONNECTION_LOST once the consumer's connection is gone. */
  async open(meta, { signal } = {}) {
    const transfer = await op(
      this,
      () => withTransferMeta(meta, (m) => native.consumerOpen(this[HANDLE], m)),
      native.consumerOpenOpWait,
      native.consumerOpenOpFd,
      signal,
    );
    return adopt(OutgoingTransfer, transfer);
  }

  /** Delivers `body` (a string is sent as UTF-8) as one whole transfer with
   * `meta`; resolves once it is written and finished. */
  async deliver(meta, body, { signal } = {}) {
    const data = bytes(body);
    await op(
      this,
      () => withTransferMeta(meta, (m) => native.consumerDeliver(this[HANDLE], m, data)),
      native.consumerDeliverOpWait,
      native.consumerDeliverOpFd,
      signal,
    );
  }

  /** Closes the connection the consumer subscribed on with `code` (one of
   * PROTOCOL.md §7's, e.g. 6 `SHUTDOWN`; above 2^62-1 sent as `INTERNAL`)
   * and `reason`, ending every subscription it holds on every path. The
   * acceptor then reports an unsubscription with a null filter. */
  close(code, reason) {
    native.consumerClose(this[HANDLE], code, reason);
  }
}

/** The sending half of one datagram flow. `drop()` closes it with FIN: the
 * receiver sees its end after the datagrams already delivered. */
export class Flow extends Handle {
  constructor() {
    super();
    noConstructor("Flow", "Peer.openFlow()");
  }

  /** Sends one datagram (a string is sent as UTF-8). Never waits: it is
   * handed to the connection, not known to arrive. Throws WEIDA_ETOO_LARGE
   * (its `max` names the limit) above `maxPayload()`, and on an ended flow
   * the error that ended it, e.g. WEIDA_EREJECTED. */
  send(payload) {
    native.flowSend(this[HANDLE], bytes(payload));
  }

  /** The largest payload `send` accepts now, or null when the connection
   * carries no datagrams. */
  maxPayload() {
    return native.flowMaxPayload(this[HANDLE]);
  }

  /** A frozen `{ sent, tooLarge, discarded, notLive }` of BigInts. */
  stats() {
    const h = this[HANDLE];
    return Object.freeze({
      sent: native.flowSent(h),
      tooLarge: native.flowTooLarge(h),
      discarded: native.flowDiscarded(h),
      notLive: native.flowNotLive(h),
    });
  }

  /** The path under the flow, frozen, or null on a local transport. */
  pathStats() {
    const p = native.flowPathStats(this[HANDLE]);
    return p === null ? null : Object.freeze(p);
  }

  /** Resets the flow `CANCELED`. Consumes it. */
  abort() {
    native.flowAbort(consume(this));
  }
}

/** The receiving half of one datagram flow, read like a queue or iterated
 * with `for await`; the loop ends with the flow, and leaving it early drops
 * the flow. `drop()` stops it `CANCELED`. */
export class IncomingFlow extends Handle {
  constructor() {
    super();
    noConstructor("IncomingFlow", "Acceptor.accept()");
  }

  /** What the sender attached, frozen: `{ endpoint, topic, contentType,
   * trace, peerFingerprint, peerChain, peerLocal }`. */
  info() {
    const m = native.incomingFlowInfo(this[HANDLE]);
    return freezeMeta({
      endpoint: m.endpoint,
      topic: m.topic,
      contentType: m.contentType,
      trace: m.trace,
      peerFingerprint: m.peerFingerprint,
      peerChain: m.peerChain,
      peerLocal: m.peerLocal,
    });
  }

  /** The next datagram, oldest first, or null once the flow ended and every
   * datagram that arrived before its end was taken. */
  async recv({ signal } = {}) {
    const handle = this[HANDLE];
    return await call(
      this,
      () => native.incomingFlowRecv(handle),
      () => native.incomingFlowFd(handle),
      signal,
    );
  }

  async *[Symbol.asyncIterator]() {
    try {
      for (let d = await this.recv(); d !== null; d = await this.recv()) {
        yield d;
      }
    } finally {
      this.drop();
    }
  }

  /** A frozen `{ received, overflow }` of BigInts. */
  stats() {
    const h = this[HANDLE];
    return Object.freeze({
      received: native.incomingFlowReceived(h),
      overflow: native.incomingFlowOverflow(h),
    });
  }

  /** The path under the flow, frozen, or null on a local transport. */
  pathStats() {
    const p = native.incomingFlowPathStats(this[HANDLE]);
    return p === null ? null : Object.freeze(p);
  }

  /** Stops the flow `REJECTED`, which the sender's next `send` throws as
   * WEIDA_EREJECTED. Consumes it. */
  refuse() {
    native.incomingFlowRefuse(consume(this));
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

/** The sending half of Push/Pull: dials pullers and pushes to them
 * round-robin. */
export class Pusher extends Handle {
  constructor() {
    super();
    noConstructor("Pusher", "Runtime.pusher()");
  }

  /** Dials `url`; the runtime redials it when the connection is lost. */
  async connect(url, { signal } = {}) {
    await op(
      this,
      () => native.pusherConnect(this[HANDLE], url),
      native.pusherConnectOpWait,
      native.pusherConnectOpFd,
      signal,
    );
  }

  /** Forgets a dialled address: whether it was dialled. */
  disconnect(url) {
    return native.pusherDisconnect(this[HANDLE], url);
  }

  /** The number of connected peers. */
  peerCount() {
    return native.pusherPeerCount(this[HANDLE]);
  }

  /** A frozen array with one frozen record per live connection, as
   * `Requester.connectionStats`. */
  connectionStats() {
    return Object.freeze(native.pusherConnectionStats(this[HANDLE]).map(freezeStats));
  }

  /** A new reader of this pusher's peer events, from now on. */
  events() {
    return adopt(PeerEvents, native.pusherEvents(this[HANDLE]));
  }

  /** The bodies the outbox discarded, a BigInt. */
  dropped() {
    return native.pusherDropped(this[HANDLE]);
  }

  /** Opens a one-way transfer to the next peer with `meta` (a
   * `TransferMeta` object, or null): written like a socket, and its
   * `finish()` hands back the delivery receipt `send` discards. Aborting
   * abandons it. */
  async open(meta, { signal } = {}) {
    const transfer = await op(
      this,
      () => withTransferMeta(meta, (m) => native.pusherOpen(this[HANDLE], m)),
      native.pusherOpenOpWait,
      native.pusherOpenOpFd,
      signal,
    );
    return adopt(OutgoingTransfer, transfer);
  }

  /** Sends `body` (a string is sent as UTF-8) as one message; resolves once
   * it is written or held in the outbox. */
  async send(body, { signal } = {}) {
    const data = bytes(body);
    await op(
      this,
      () => native.pusherSend(this[HANDLE], data),
      native.pusherSendOpWait,
      native.pusherSendOpFd,
      signal,
    );
  }

  /** `send` with metadata (a `TransferMeta` object, or null), copied when
   * the send starts. */
  async sendWith(meta, body, { signal } = {}) {
    const data = bytes(body);
    await op(
      this,
      () => withTransferMeta(meta, (m) => native.pusherSendWith(this[HANDLE], m, data)),
      native.pusherSendOpWait,
      native.pusherSendOpFd,
      signal,
    );
  }
}

/** The receiving half of Push/Pull, serving one endpoint path. */
export class Puller extends Handle {
  constructor() {
    super();
    noConstructor("Puller", "Listener.puller()");
  }

  path() {
    return native.pullerPath(this[HANDLE]);
  }

  /** The next pushed transfer, as soon as its header arrives. */
  async recv({ signal } = {}) {
    const handle = this[HANDLE];
    const transfer = await call(
      this,
      () => native.pullerRecv(handle),
      () => native.pullerFd(handle),
      signal,
    );
    return adopt(IncomingTransfer, transfer);
  }
}

/** The publishing half of Pub/Sub, serving one endpoint path. */
export class Publisher extends Handle {
  constructor() {
    super();
    noConstructor("Publisher", "Listener.publisher()");
  }

  path() {
    return native.publisherPath(this[HANDLE]);
  }

  /** Fans `payload` (a string is sent as UTF-8) out to every subscriber
   * whose filter matches `topic`, without waiting on any: how many it was
   * enqueued to. One whose budget is exhausted loses it, counted by
   * `dropped()`. */
  publish(topic, payload) {
    return native.publisherPublish(this[HANDLE], topic, bytes(payload));
  }

  /** `publish` propagating `trace`, a `traceparent` text. */
  publishWithTrace(topic, payload, trace) {
    return native.publisherPublishWithTrace(this[HANDLE], topic, bytes(payload), trace);
  }

  /** A streamed publish on `topic` to the subscribers matched now. */
  open(topic) {
    return adopt(FanOut, native.publisherOpen(this[HANDLE], topic));
  }

  /** `open` propagating `trace`, a `traceparent` text. */
  openWithTrace(topic, trace) {
    return adopt(FanOut, native.publisherOpenWithTrace(this[HANDLE], topic, trace));
  }

  /** The connections subscribed to this publisher. */
  subscriberCount() {
    return native.publisherSubscriberCount(this[HANDLE]);
  }

  /** The filters registered on this publisher, summed over subscribers. */
  filterCount() {
    return native.publisherFilterCount(this[HANDLE]);
  }

  /** The copies dropped because a subscriber could not take them, a BigInt. */
  dropped() {
    return native.publisherDropped(this[HANDLE]);
  }

  /** What was dropped on `topic`, a frozen `{ topic, subscriberBudget,
   * subscriberQueue, noParkedConnection, superseded, expired, tooLarge,
   * noDatagrams, layersCut }` of BigInts, or null when nothing was. */
  droppedOn(topic) {
    const [record] = native.publisherDroppedOn(this[HANDLE], topic);
    return record === undefined ? null : Object.freeze(record);
  }

  /** A frozen array with one frozen record per topic that lost a copy. */
  drops() {
    return Object.freeze(native.publisherDrops(this[HANDLE]).map((r) => Object.freeze(r)));
  }
}

/** A publish in progress: one payload written chunk by chunk, a stream per
 * subscriber. Dropping it without `finish()` aborts every copy. */
export class FanOut extends Handle {
  constructor() {
    super();
    noConstructor("FanOut", "Publisher.open()");
  }

  topic() {
    return native.fanOutTopic(this[HANDLE]);
  }

  /** The subscribers still receiving this transfer. */
  subscribers() {
    return native.fanOutSubscribers(this[HANDLE]);
  }

  /** Writes `chunk` to every subscriber still receiving, waiting up to
   * `limitMs` for all of those with no room together: the subscribers left.
   * Until it settles every other call on the fan-out throws WEIDA_EINVAL.
   * Aborting abandons the transfer: later writes throw WEIDA_ECANCELED and
   * `finish()` aborts every copy. */
  async writeWithin(chunk, limitMs, { signal } = {}) {
    const data = bytes(chunk);
    return op(
      this,
      () => native.fanOutWriteWithin(this[HANDLE], data, limitMs),
      native.fanOutWriteWithinOpWait,
      native.fanOutWriteWithinOpFd,
      signal,
    );
  }

  /** Writes `chunk` without waiting: one with no room loses the transfer.
   * The subscribers left. */
  writeNow(chunk) {
    return native.fanOutWriteNow(this[HANDLE], bytes(chunk));
  }

  /** Ends the transfer: the subscribers that received all of it. Consumes
   * the fan-out. */
  finish() {
    return native.fanOutFinish(consume(this));
  }
}

/** The subscribing half of Pub/Sub. */
export class Subscriber extends Handle {
  constructor() {
    super();
    noConstructor("Subscriber", "Runtime.subscriber()");
  }

  /** Dials `url` and subscribes there to every filter registered so far;
   * the runtime redials it, filters included, when the connection is lost. */
  async connect(url, { signal } = {}) {
    await op(
      this,
      () => native.subscriberConnect(this[HANDLE], url),
      native.subscriberConnectOpWait,
      native.subscriberConnectOpFd,
      signal,
    );
  }

  /** Forgets a dialled address: whether it was dialled. */
  disconnect(url) {
    return native.subscriberDisconnect(this[HANDLE], url);
  }

  /** The number of connected peers. */
  peerCount() {
    return native.subscriberPeerCount(this[HANDLE]);
  }

  /** A frozen array with one frozen record per live connection, as
   * `Requester.connectionStats`. */
  connectionStats() {
    return Object.freeze(native.subscriberConnectionStats(this[HANDLE]).map(freezeStats));
  }

  /** A new reader of this subscriber's peer events, from now on. */
  events() {
    return adopt(PeerEvents, native.subscriberEvents(this[HANDLE]));
  }

  /** Registers interest in every topic `filter` matches (PROTOCOL.md §6.4:
   * `*` one segment, a trailing `#` any number). A filter outside the
   * grammar rejects with WEIDA_EPROTOCOL. */
  async subscribe(filter, { signal } = {}) {
    await op(
      this,
      () => native.subscriberSubscribe(this[HANDLE], filter),
      native.subscriberSubscribeOpWait,
      native.subscriberSubscribeOpFd,
      signal,
    );
  }

  /** Withdraws `filter`; an unknown one is ignored. */
  async unsubscribe(filter, { signal } = {}) {
    await op(
      this,
      () => native.subscriberUnsubscribe(this[HANDLE], filter),
      native.subscriberUnsubscribeOpWait,
      native.subscriberUnsubscribeOpFd,
      signal,
    );
  }

  /** States how many messages this subscriber accepts in total on `filter`:
   * the credit a queue serving the path consults, absolute and cumulative. */
  async grant(filter, limit, { signal } = {}) {
    await op(
      this,
      () => native.subscriberGrant(this[HANDLE], filter, limit),
      native.subscriberGrantOpWait,
      native.subscriberGrantOpFd,
      signal,
    );
  }

  /** The filters this subscriber registered. */
  filterCount() {
    return native.subscriberFilterCount(this[HANDLE]);
  }

  /** The next published copy, as soon as its header arrives; its topic is
   * `meta().topic`. */
  async recv({ signal } = {}) {
    const handle = this[HANDLE];
    const transfer = await call(
      this,
      () => native.subscriberRecv(handle),
      () => native.subscriberFd(handle),
      signal,
    );
    return adopt(IncomingTransfer, transfer);
  }
}

/** Either half of PAIR: one peer, one-way transfers both ways. A dialling
 * pair (`Runtime.pair`) dials its peer; a bound one (`Listener.pair`) is
 * dialled, and its first send or open waits until the peer has sent. */
export class Paired extends Handle {
  constructor() {
    super();
    noConstructor("Paired", "Runtime.pair() or Listener.pair()");
  }

  /** The bound pair's path; "" on a dialling pair. */
  path() {
    return native.pairedPath(this[HANDLE]);
  }

  /** Dials `url`, once: a second address rejects with
   * WEIDA_ELIMIT_EXCEEDED, a bound pair with WEIDA_EUNSUPPORTED. */
  async connect(url, { signal } = {}) {
    await op(
      this,
      () => native.pairedConnect(this[HANDLE], url),
      native.pairedConnectOpWait,
      native.pairedConnectOpFd,
      signal,
    );
  }

  /** The number of connected peers, 0 or 1; 0 on a bound pair. */
  peerCount() {
    return native.pairedPeerCount(this[HANDLE]);
  }

  /** A frozen array with one frozen record per live connection, as
   * `Requester.connectionStats`; empty on a bound pair. */
  connectionStats() {
    return Object.freeze(native.pairedConnectionStats(this[HANDLE]).map(freezeStats));
  }

  /** A new reader of a dialling pair's peer events, from now on; null on a
   * bound pair. */
  events() {
    const handle = native.pairedEvents(this[HANDLE]);
    return handle === null ? null : adopt(PeerEvents, handle);
  }

  /** The bodies a dialling pair's outbox discarded, a BigInt. */
  dropped() {
    return native.pairedDropped(this[HANDLE]);
  }

  /** Opens a one-way transfer to the peer with `meta` (a `TransferMeta`
   * object, or null): written like a socket, and its `finish()` hands back
   * the delivery receipt `send` discards. Aborting abandons it. */
  async open(meta, { signal } = {}) {
    const transfer = await op(
      this,
      () => withTransferMeta(meta, (m) => native.pairedOpen(this[HANDLE], m)),
      native.pairedOpenOpWait,
      native.pairedOpenOpFd,
      signal,
    );
    return adopt(OutgoingTransfer, transfer);
  }

  /** Sends `body` (a string is sent as UTF-8) as one message; resolves once
   * it is written or held in the outbox. */
  async send(body, { signal } = {}) {
    const data = bytes(body);
    await op(
      this,
      () => native.pairedSend(this[HANDLE], data),
      native.pairedSendOpWait,
      native.pairedSendOpFd,
      signal,
    );
  }

  /** `send` with metadata (a `TransferMeta` object, or null), copied when
   * the send starts. */
  async sendWith(meta, body, { signal } = {}) {
    const data = bytes(body);
    await op(
      this,
      () => withTransferMeta(meta, (m) => native.pairedSendWith(this[HANDLE], m, data)),
      native.pairedSendOpWait,
      native.pairedSendOpFd,
      signal,
    );
  }

  /** The next transfer from the peer, as soon as its header arrives. */
  async recv({ signal } = {}) {
    const handle = this[HANDLE];
    const transfer = await call(
      this,
      () => native.pairedRecv(handle),
      () => native.pairedFd(handle),
      signal,
    );
    return adopt(IncomingTransfer, transfer);
  }
}

/** The asking half of SURVEY: dials respondents and asks every one of
 * them. */
export class Surveyor extends Handle {
  constructor() {
    super();
    noConstructor("Surveyor", "Runtime.surveyor()");
  }

  /** Dials the respondent at `url`, adding it to the set every survey
   * asks; the runtime redials it when the connection is lost. */
  async connect(url, { signal } = {}) {
    await op(
      this,
      () => native.surveyorConnect(this[HANDLE], url),
      native.surveyorConnectOpWait,
      native.surveyorConnectOpFd,
      signal,
    );
  }

  /** The number of connected respondents. */
  peerCount() {
    return native.surveyorPeerCount(this[HANDLE]);
  }

  /** A frozen array with one frozen record per live connection, as
   * `Requester.connectionStats`. */
  connectionStats() {
    return Object.freeze(native.surveyorConnectionStats(this[HANDLE]).map(freezeStats));
  }

  /** Asks every connected respondent `body` (a string is sent as UTF-8)
   * and collects their answers for `deadlineMs`, which bounds the asking
   * too: resolves with the run once every respondent was asked. */
  async survey(body, deadlineMs, { signal } = {}) {
    const data = bytes(body);
    const run = await op(
      this,
      () => native.surveyorSurvey(this[HANDLE], data, deadlineMs),
      native.surveyorSurveyOpWait,
      native.surveyorSurveyOpFd,
      signal,
    );
    return adopt(SurveyRun, run);
  }

  /** `survey` with metadata (a `TransferMeta` object, or null), copied
   * when the survey starts. */
  async surveyWith(meta, body, deadlineMs, { signal } = {}) {
    const data = bytes(body);
    const run = await op(
      this,
      () =>
        withTransferMeta(meta, (m) =>
          native.surveyorSurveyWith(this[HANDLE], m, data, deadlineMs),
        ),
      native.surveyorSurveyOpWait,
      native.surveyorSurveyOpFd,
      signal,
    );
    return adopt(SurveyRun, run);
  }
}

/** One survey in progress: its answers, and what the deadline cost.
 * Dropping it resets every exchange not yet answered. */
export class SurveyRun extends Handle {
  constructor() {
    super();
    noConstructor("SurveyRun", "Surveyor.survey()");
  }

  /** The next answer's bytes, at most `maxBytes` of them, or null once the
   * deadline passed or every respondent answered. An answer that failed
   * rejects and the run goes on. An aborted call leaves its read in flight
   * for the next one, which keeps the first call's `maxBytes`. */
  async next(maxBytes, { signal } = {}) {
    const handle = this[HANDLE];
    return call(
      this,
      () => native.surveyRunNext(handle, maxBytes),
      () => native.surveyRunFd(handle),
      signal,
    );
  }

  /** The answers that arrived after the deadline, a BigInt. */
  late() {
    return native.surveyRunLate(this[HANDLE]);
  }

  /** How many respondents were asked. */
  respondents() {
    return native.surveyRunRespondents(this[HANDLE]);
  }
}

/** The answering half of SURVEY, serving one endpoint path. */
export class Respondent extends Handle {
  constructor() {
    super();
    noConstructor("Respondent", "Listener.respondent()");
  }

  path() {
    return native.respondentPath(this[HANDLE]);
  }

  /** The next question, as soon as its header arrives: an
   * `IncomingRequest` answered as a replier's is. */
  async accept({ signal } = {}) {
    const handle = this[HANDLE];
    const request = await call(
      this,
      () => native.respondentAccept(handle),
      () => native.respondentFd(handle),
      signal,
    );
    return adopt(IncomingRequest, request);
  }
}

/** A blob: its root (`BLOB_ID_LEN` bytes, the BLAKE3 hash of its bytes),
 * its size in bytes and its piece size, `2 ** pieceLog2` bytes. More than
 * `MAX_PIECES` pieces, a piece size outside `PIECE_SIZE_MIN_LOG2` to
 * `PIECE_SIZE_MAX_LOG2`, or an empty blob whose root is not the hash of no
 * bytes throws WEIDA_EINVAL. */
export class Blob extends Handle {
  constructor(root, size, pieceLog2 = PIECE_SIZE_DEFAULT_LOG2) {
    super();
    init(this, native.blobNew(root, size, pieceLog2));
  }

  /** The hash of the whole blob, a fresh Uint8Array. */
  root() {
    return native.blobRoot(this[HANDLE]);
  }

  /** The length in bytes, a BigInt. */
  size() {
    return native.blobSize(this[HANDLE]);
  }

  /** The piece size's base-2 logarithm. */
  pieceLog2() {
    return native.blobPieceLog2(this[HANDLE]);
  }

  /** The number of pieces: the size over the piece size, rounded up. */
  count() {
    return native.blobCount(this[HANDLE]);
  }

  /** The byte range of piece `index`, a frozen `{ start, end }` of BigInts,
   * end exclusive; null past the last piece. */
  span(index) {
    const span = native.blobSpan(this[HANDLE], index);
    return span === null ? null : Object.freeze(span);
  }
}

/** The piece hashes of a blob, checked against its root. */
export class PieceList extends Handle {
  constructor() {
    super();
    noConstructor("PieceList", "PieceList.of(), .parse(), TreeHasher.finish() or Fetch.list()");
  }

  /** The piece list of `data` (a string is hashed as UTF-8) cut into pieces
   * of `2 ** pieceLog2` bytes; its `blob()` holds the root. */
  static of(data, pieceLog2 = PIECE_SIZE_DEFAULT_LOG2) {
    return adopt(PieceList, native.pieceListOf(bytes(data), pieceLog2));
  }

  /** The piece list of `blob` from its bytes, as `toBytes()` gives them:
   * WEIDA_EINVAL unless they hash to the blob's root. */
  static parse(blob, data) {
    return adopt(PieceList, native.pieceListParse(handleOf(blob, Blob), data));
  }

  /** The blob this list belongs to. */
  blob() {
    return adopt(Blob, native.pieceListBlob(this[HANDLE]));
  }

  /** The chaining values one after the other, 32 bytes each, a copy; empty
   * for a blob of at most one piece. */
  toBytes() {
    return native.pieceListToBytes(this[HANDLE]);
  }

  /** Whether `piece` (a string is checked as UTF-8) is piece `index` of
   * the blob; false past the last piece. */
  check(index, piece) {
    return native.pieceListCheck(this[HANDLE], index, bytes(piece));
  }
}

/** Builds a piece list in one pass over a blob's bytes, fed in order. */
export class TreeHasher extends Handle {
  constructor(pieceLog2 = PIECE_SIZE_DEFAULT_LOG2) {
    super();
    init(this, native.treeHasherNew(pieceLog2));
  }

  /** Feeds the next bytes of the blob (a string as UTF-8); returns this
   * hasher. */
  update(data) {
    native.treeHasherUpdate(this[HANDLE], bytes(data));
    return this;
  }

  /** Ends the bytes: their piece list, whose `blob()` holds the root.
   * Consumes the hasher. */
  finish() {
    return adopt(PieceList, native.treeHasherFinish(consume(this)));
  }
}

/** A map of held pieces, one bit per piece. */
export class Bits extends Handle {
  /** `len` bits, all clear. */
  constructor(len) {
    super();
    init(this, native.bitsNew(len));
  }

  /** `len` bits, all set. */
  static full(len) {
    return adopt(Bits, native.bitsFull(len));
  }

  /** `len` bits from the bytes they travel as: bit `i` is bit `i % 8`,
   * least significant first, of byte `i / 8`, padding bits clear. */
  static fromBytes(len, data) {
    return adopt(Bits, native.bitsFromBytes(len, data));
  }

  /** The bits as they travel, a copy. */
  asBytes() {
    return native.bitsAsBytes(this[HANDLE]);
  }

  len() {
    return native.bitsLen(this[HANDLE]);
  }

  /** Whether bit `index` is set; false past the end. */
  get(index) {
    return native.bitsGet(this[HANDLE], index);
  }

  /** Sets or clears bit `index`; past the end it does nothing. Returns
   * this map. */
  set(index, on) {
    native.bitsSet(this[HANDLE], index, on);
    return this;
  }

  /** How many bits are set. */
  ones() {
    return native.bitsOnes(this[HANDLE]);
  }

  /** Whether every bit is set; true for no bits at all. */
  all() {
    return native.bitsAll(this[HANDLE]);
  }
}

/** The dialling half of SWARM: fetches one blob from every connected seeder
 * at once, each piece verified against the root. */
export class Swarm extends Handle {
  constructor() {
    super();
    noConstructor("Swarm", "Runtime.swarm()");
  }

  /** Dials the seeder at `url`; the runtime redials it when the connection
   * is lost. A seeder connected during a fetch joins it at its next read. */
  async connect(url, { signal } = {}) {
    await op(
      this,
      () => native.swarmConnect(this[HANDLE], url),
      native.swarmConnectOpWait,
      native.swarmConnectOpFd,
      signal,
    );
  }

  /** Forgets a dialled address: whether it was dialled. */
  disconnect(url) {
    return native.swarmDisconnect(this[HANDLE], url);
  }

  /** The number of connected seeders. */
  peerCount() {
    return native.swarmPeerCount(this[HANDLE]);
  }

  /** A frozen array with one frozen record per live connection, as
   * `Requester.connectionStats`. */
  connectionStats() {
    return Object.freeze(native.swarmConnectionStats(this[HANDLE]).map(freezeStats));
  }

  /** A new reader of this swarm's events, from now on. */
  events() {
    return adopt(PeerEvents, native.swarmEvents(this[HANDLE]));
  }

  /** Starts fetching the pieces of `blob` that `have` (a `Bits` of one bit
   * per piece) lacks, ordered as `options` says: `{ list?, stallMs?,
   * minRate?, perPeer?, maxInflight?, maxPeers?, haveRefreshMs? }`, or the
   * defaults. `have` and the options are copied. Nothing is ordered before
   * the first `next`. */
  fetch(blob, have, options = null) {
    return withFields(options, native.fetchOptionsNew, FETCH_FIELDS, "FetchOptions", (o) =>
      adopt(
        Fetch,
        native.swarmFetch(this[HANDLE], handleOf(blob, Blob), handleOf(have, Bits), o),
      ),
    );
  }
}

/** One blob being fetched: its verified pieces, in any order. Dropping it
 * aborts every order in flight. */
export class Fetch extends Handle {
  constructor() {
    super();
    noConstructor("Fetch", "Swarm.fetch()");
  }

  /** The blob being fetched. */
  blob() {
    return adopt(Blob, native.fetchBlob(this[HANDLE]));
  }

  /** The piece list once it is known and verified, or null: keep it to
   * resume without asking for it (`{ list }`). WEIDA_EINVAL while a `next`
   * is pending. */
  list() {
    const list = native.fetchList(this[HANDLE]);
    return list === null ? null : adopt(PieceList, list);
  }

  /** The pieces the caller holds: those it started with and those this
   * fetch yielded, a copy. WEIDA_EINVAL while a `next` is pending. */
  delivered() {
    return adopt(Bits, native.fetchDelivered(this[HANDLE]));
  }

  /** The pieces ordered and not yet taken, a copy. WEIDA_EINVAL while a
   * `next` is pending. */
  inFlight() {
    return adopt(Bits, native.fetchInFlight(this[HANDLE]));
  }

  /** The next verified piece, a frozen `{ index, bytes }` copied out at
   * once, or null once every piece is held. Rejects with
   * WEIDA_ENOT_CONNECTED when no seeder is left that has not failed this
   * fetch; connecting one and calling again resumes. Taking a piece frees
   * its order's credit, so a caller that takes pieces only as fast as it
   * stores them is the fetch's backpressure. An aborted call leaves its
   * read in flight for the next one. */
  async next({ signal } = {}) {
    const handle = this[HANDLE];
    const piece = await call(
      this,
      () => native.fetchNext(handle),
      () => native.fetchFd(handle),
      signal,
    );
    return piece === null ? null : Object.freeze(piece);
  }

  /** Every piece until the end; the fetch stays alive, so its list and
   * maps can be read after the loop. */
  async *[Symbol.asyncIterator]() {
    for (let piece = await this.next(); piece !== null; piece = await this.next()) {
      yield piece;
    }
  }
}

/** The bound half of SWARM, serving one endpoint path. */
export class Seeder extends Handle {
  constructor() {
    super();
    noConstructor("Seeder", "Listener.seeder()");
  }

  path() {
    return native.seederPath(this[HANDLE]);
  }

  /** The next fetch's session, always with a well-formed open: admit it
   * from its blob and metadata, or decline it. */
  async accept({ signal } = {}) {
    const handle = this[HANDLE];
    const session = await call(
      this,
      () => native.seederAccept(handle),
      () => native.seederFd(handle),
      signal,
    );
    return adopt(SeedSession, session);
  }
}

/** One fetch's session with this seeder, about one blob. Its calls are one
 * at a time (WEIDA_EINVAL otherwise), and each order is answered before
 * the next `next`. Dropping it ends the session. */
export class SeedSession extends Handle {
  constructor() {
    super();
    noConstructor("SeedSession", "Seeder.accept()");
  }

  /** The blob this session is about. */
  blob() {
    return adopt(Blob, native.seedSessionBlob(this[HANDLE]));
  }

  /** The exchange's metadata, frozen, as `IncomingRequest.meta`. */
  meta() {
    return freezeMeta(native.seedSessionMeta(this[HANDLE]));
  }

  /** The number of the connection this session arrived on, a BigInt. */
  connection() {
    return native.seedSessionConnection(this[HANDLE]);
  }

  /** The next order, a frozen `{ kind, index }` (`kind` one of
   * "WEIDA_ASK_LIST", "WEIDA_ASK_HAVE", "WEIDA_ASK_PIECE", `index` the piece
   * of a PIECE and 0 otherwise), or null once the fetch finished its
   * orders. Waits as long as the fetch sends none. Rejects with
   * WEIDA_EPROTOCOL while the order before is unanswered. An aborted call
   * leaves its read in flight for the next one. */
  async next({ signal } = {}) {
    const handle = this[HANDLE];
    const ask = await call(
      this,
      () => native.seedSessionNext(handle),
      () => native.seedSessionFd(handle),
      signal,
    );
    return ask === null ? null : Object.freeze(ask);
  }

  /** Answers LIST with the blob's piece list. */
  async answerList(list, { signal } = {}) {
    await op(
      this,
      () => native.seedSessionAnswerList(this[HANDLE], handleOf(list, PieceList)),
      native.seedSessionAnswerListOpWait,
      native.seedSessionAnswerListOpFd,
      signal,
    );
  }

  /** Answers HAVE with the pieces this seeder can serve now, a `Bits`. */
  async answerHave(have, { signal } = {}) {
    await op(
      this,
      () => native.seedSessionAnswerHave(this[HANDLE], handleOf(have, Bits)),
      native.seedSessionAnswerHaveOpWait,
      native.seedSessionAnswerHaveOpFd,
      signal,
    );
  }

  /** Answers PIECE with the piece's bytes, copied; the fetch checks them
   * against the root. */
  async answerPiece(data, { signal } = {}) {
    const piece = bytes(data);
    await op(
      this,
      () => native.seedSessionAnswerPiece(this[HANDLE], piece),
      native.seedSessionAnswerPieceOpWait,
      native.seedSessionAnswerPieceOpFd,
      signal,
    );
  }

  /** Ends the session: a blob this seeder does not serve or a piece it
   * does not hold; the fetch drops this seeder for the blob. Consumes the
   * session. */
  async decline({ signal } = {}) {
    await op(
      this,
      () => native.seedSessionDecline(consume(this)),
      native.seedSessionDeclineOpWait,
      native.seedSessionDeclineOpFd,
      signal,
    );
  }
}

/** One member of a BUS: accepts on its path and dials the other members;
 * a message goes to every member it dialled, never to itself, and nothing
 * is relayed. */
export class BusMember extends Handle {
  constructor() {
    super();
    noConstructor("BusMember", "Listener.bus()");
  }

  path() {
    return native.busMemberPath(this[HANDLE]);
  }

  /** Joins the member at `url`; the runtime redials it when the connection
   * is lost. */
  async connect(url, { signal } = {}) {
    await op(
      this,
      () => native.busMemberConnect(this[HANDLE], url),
      native.busMemberConnectOpWait,
      native.busMemberConnectOpFd,
      signal,
    );
  }

  /** The number of members this one joined. */
  peerCount() {
    return native.busMemberPeerCount(this[HANDLE]);
  }

  /** A frozen array with one frozen record per live connection, as
   * `Requester.connectionStats`. */
  connectionStats() {
    return Object.freeze(native.busMemberConnectionStats(this[HANDLE]).map(freezeStats));
  }

  /** Sends `body` (a string is sent as UTF-8) to every member this one
   * dialled: how many it reached. A member that cannot take a copy loses
   * it, counted by `dropped()`. */
  async send(body, { signal } = {}) {
    const data = bytes(body);
    return op(
      this,
      () => native.busMemberSend(this[HANDLE], data),
      native.busMemberSendOpWait,
      native.busMemberSendOpFd,
      signal,
    );
  }

  /** `send` with metadata (a `TransferMeta` object, or null), copied when
   * the send starts. */
  async sendWith(meta, body, { signal } = {}) {
    const data = bytes(body);
    return op(
      this,
      () => withTransferMeta(meta, (m) => native.busMemberSendWith(this[HANDLE], m, data)),
      native.busMemberSendOpWait,
      native.busMemberSendOpFd,
      signal,
    );
  }

  /** The next message from another member, as soon as its header
   * arrives. */
  async recv({ signal } = {}) {
    const handle = this[HANDLE];
    const transfer = await call(
      this,
      () => native.busMemberRecv(handle),
      () => native.busMemberFd(handle),
      signal,
    );
    return adopt(IncomingTransfer, transfer);
  }

  /** The copies that never reached a member, a BigInt. */
  dropped() {
    return native.busMemberDropped(this[HANDLE]);
  }
}

/** The sending half of RADIO/DISH, serving one endpoint path. Dropping it
 * releases the path. */
export class Radio extends Handle {
  constructor() {
    super();
    noConstructor("Radio", "Listener.radio()");
  }

  path() {
    return native.radioPath(this[HANDLE]);
  }

  /** Opens the next segment on `topic` to the dishes joined now, on
   * `terms` `{ maxAgeMs?, priority?, followsUpstream? }`. Opening it resets
   * every unfinished layer of the topic's previous segment. */
  segment(topic, terms = null) {
    const handle = withFields(terms, native.segmentTermsNew, SEGMENT_FIELDS, "SegmentTerms", (t) =>
      native.radioSegment(this[HANDLE], topic, t),
    );
    return adopt(Segment, handle);
  }

  /** Sends `payload` (a string is sent as UTF-8) as one datagram segment to
   * every dish joined to `topic`: how many it was handed to. A dish that
   * carries no datagrams counts a `noDatagrams` drop. */
  datagram(topic, payload) {
    return native.radioDatagram(this[HANDLE], topic, bytes(payload));
  }

  /** The dishes joined. */
  dishCount() {
    return native.radioDishCount(this[HANDLE]);
  }

  /** The copies dropped, over topics and causes, a BigInt. */
  dropped() {
    return native.radioDropped(this[HANDLE]);
  }

  /** What was dropped on `topic`, a frozen record as
   * `Publisher.droppedOn`, or null when nothing was. */
  droppedOn(topic) {
    const [record] = native.radioDroppedOn(this[HANDLE], topic);
    return record === undefined ? null : Object.freeze(record);
  }

  /** A frozen array with one frozen record per topic that lost a copy. */
  drops() {
    return Object.freeze(native.radioDrops(this[HANDLE]).map((r) => Object.freeze(r)));
  }

  /** Withdraws the joins on `filter` of the dishes whose key is
   * `fingerprint` (`sha256:` text): how many went. */
  evict(fingerprint, filter) {
    return native.radioEvict(this[HANDLE], fingerprint, filter);
  }

  /** A frozen array with one frozen `{ peerFingerprint, subscriberBudget,
   * subscriberQueue, superseded, expired, tooLarge, noDatagrams, layersCut }`
   * per joined dish connection, the counts BigInts. */
  dishDrops() {
    return Object.freeze(native.radioDishDrops(this[HANDLE]).map((r) => Object.freeze(r)));
  }
}

/** One open segment of a radio: layers written without waiting. Dropping it
 * without `finish()` resets every unfinished layer. */
export class Segment extends Handle {
  constructor() {
    super();
    noConstructor("Segment", "Radio.segment()");
  }

  /** Its number on its topic, a BigInt; the first is 0n. */
  number() {
    return native.segmentNumber(this[HANDLE]);
  }

  topic() {
    return native.segmentTopic(this[HANDLE]);
  }

  /** Writes `chunk` to layer 0: how many copies took it. */
  write(chunk) {
    return native.segmentWrite(this[HANDLE], bytes(chunk));
  }

  /** Writes `chunk` to `layer` (0 to 15) without waiting: how many copies
   * took it. A copy without room loses this layer and every one above it;
   * a copy capped below `layer` is skipped, which is not a drop. */
  writeLayer(layer, chunk) {
    return native.segmentWriteLayer(this[HANDLE], layer, bytes(chunk));
  }

  /** Finishes `layer`; a later write to it throws WEIDA_ERUNTIME. */
  finishLayer(layer) {
    native.segmentFinishLayer(this[HANDLE], layer);
  }

  /** Finishes every open layer: how many copies still hold layer 0.
   * Consumes the segment. */
  finish() {
    return native.segmentFinish(consume(this));
  }
}

/** The receiving half of RADIO/DISH. */
export class Dish extends Handle {
  constructor() {
    super();
    noConstructor("Dish", "Runtime.dish()");
  }

  /** Dials a radio at `url` and joins there every filter joined so far; a
   * redial joins again, and what was sent in between is gone. */
  async connect(url, { signal } = {}) {
    await op(
      this,
      () => native.dishConnect(this[HANDLE], url),
      native.dishConnectOpWait,
      native.dishConnectOpFd,
      signal,
    );
  }

  /** The number of connected radios. */
  peerCount() {
    return native.dishPeerCount(this[HANDLE]);
  }

  /** A frozen array with one frozen record per live connection, as
   * `Requester.connectionStats`. */
  connectionStats() {
    return Object.freeze(native.dishConnectionStats(this[HANDLE]).map(freezeStats));
  }

  /** A new reader of this dish's peer events, from now on. */
  events() {
    return adopt(PeerEvents, native.dishEvents(this[HANDLE]));
  }

  /** Joins every topic `filter` matches on `terms` `{ maxAgeMs?, maxLayer? }`.
   * A `maxLayer` of 16 or above rejects with WEIDA_ELIMIT_EXCEEDED, a
   * filter outside the grammar with WEIDA_EPROTOCOL. */
  async join(filter, terms = null, { signal } = {}) {
    await op(
      this,
      () =>
        withFields(terms, native.joinTermsNew, JOIN_FIELDS, "JoinTerms", (t) =>
          native.dishJoin(this[HANDLE], filter, t),
        ),
      native.dishJoinOpWait,
      native.dishJoinOpFd,
      signal,
    );
  }

  /** Leaves `filter`; an unknown one is ignored. */
  async leave(filter, { signal } = {}) {
    await op(
      this,
      () => native.dishLeave(this[HANDLE], filter),
      native.dishLeaveOpWait,
      native.dishLeaveOpFd,
      signal,
    );
  }

  /** The next segment, a frozen `{ kind: "WEIDA_RECEIVED_SEGMENT", transfer }`
   * whose topic, segment and layer are on `transfer.meta()`, or a frozen
   * `{ kind: "WEIDA_RECEIVED_DATAGRAM", topic, segment, payload }`. */
  async recv({ signal } = {}) {
    const handle = this[HANDLE];
    const r = await call(
      this,
      () => native.dishRecv(handle),
      () => native.dishFd(handle),
      signal,
    );
    if (r.kind === "WEIDA_RECEIVED_SEGMENT") {
      return Object.freeze({ kind: r.kind, transfer: adopt(IncomingTransfer, r.transfer) });
    }
    return Object.freeze(r);
  }

  /** Segments discarded because a newer one on their topic was already
   * delivered, a BigInt. */
  stale() {
    return native.dishStale(this[HANDLE]);
  }

  /** Segments discarded because the receive queue was full, a BigInt. */
  overflow() {
    return native.dishOverflow(this[HANDLE]);
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
    noConstructor("IncomingRequest", "Replier.accept() or Respondent.accept()");
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

  /** A new reporter for the cursor levels the sender ordered, on every
   * call, so take one; null when it ordered none. Take it before
   * `collect()`: it outlives the transfer. */
  reporter() {
    const r = native.incomingTransferReporter(this[HANDLE]);
    return r === null ? null : adopt(Reporter, r);
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

  /** The queue of cursor sets the receiver reports for this transfer, the
   * first time; null after that and when its metadata ordered no levels.
   * Take it before `finish()`: it outlives the transfer, since Accepted
   * arrives after the payload's end. */
  cursors() {
    const c = native.outgoingTransferCursors(this[HANDLE]);
    return c === null ? null : adopt(Cursors, c);
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
