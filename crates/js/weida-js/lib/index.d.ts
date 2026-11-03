// Declarations of lib/index.js (docs/libraries/weida-js.md).

import type { KeyObject, X509Certificate } from "node:crypto";

/** The options every waiting method takes last. */
export interface WaitOptions {
  /** Aborting it rejects the wait with `signal.reason` and cancels the
   * operation; `AbortSignal.timeout(ms)` is the timeout. */
  signal?: AbortSignal;
}

/** Bytes in: a Uint8Array (a Buffer is one), or a string sent as UTF-8. */
export type Bytes = Uint8Array | string;

/** The library's version, e.g. "0.1.0-alpha.11-dev". */
export function version(): string;

/** The description of a status code, either sign; "unknown error" for one
 * libweida does not define. */
export function strerror(code: number): string;

/** A fresh root trace context as W3C `traceparent` text. */
export function newTrace(): string;

/** A count, a duration in milliseconds or an optional number. */
export type Count = number | bigint;

// Enumerations travel as the names of the weida.h constants.
export type Congestion = "WEIDA_CONGESTION_CUBIC" | "WEIDA_CONGESTION_NEW_RENO" | "WEIDA_CONGESTION_BBR";
export type Discovery = "WEIDA_DISCOVERY_AWARE" | "WEIDA_DISCOVERY_SINGLE";
export type OutboxFull = "WEIDA_OUTBOX_FULL_BLOCK" | "WEIDA_OUTBOX_FULL_DROP" | "WEIDA_OUTBOX_FULL_REJECT";
export type DeliveryLevel =
  | "WEIDA_DELIVERY_LEVEL_BEST_EFFORT"
  | "WEIDA_DELIVERY_LEVEL_AT_MOST_ONCE"
  | "WEIDA_DELIVERY_LEVEL_AT_LEAST_ONCE";
export type Acknowledgement =
  | "WEIDA_ACKNOWLEDGEMENT_NONE"
  | "WEIDA_ACKNOWLEDGEMENT_TRANSPORT_RECEIPT"
  | "WEIDA_ACKNOWLEDGEMENT_ACCEPTED"
  | "WEIDA_ACKNOWLEDGEMENT_STORED"
  | "WEIDA_ACKNOWLEDGEMENT_REPLICATED"
  | "WEIDA_ACKNOWLEDGEMENT_PROCESSED";
export type Durability = "WEIDA_DURABILITY_WRITTEN" | "WEIDA_DURABILITY_FLUSHED";
export type OrderingMode =
  | "WEIDA_ORDERING_MODE_NONE"
  | "WEIDA_ORDERING_MODE_PER_PRODUCER_DETECT"
  | "WEIDA_ORDERING_MODE_PER_PRODUCER_REASSEMBLE"
  | "WEIDA_ORDERING_MODE_PER_KEY"
  | "WEIDA_ORDERING_MODE_TOTAL";
export type Deduplication =
  | "WEIDA_DEDUPLICATION_NONE"
  | "WEIDA_DEDUPLICATION_BOUNDED"
  | "WEIDA_DEDUPLICATION_DURABLE";
export type Backpressure =
  | "WEIDA_BACKPRESSURE_BLOCK"
  | "WEIDA_BACKPRESSURE_REJECT"
  | "WEIDA_BACKPRESSURE_DROP"
  | "WEIDA_BACKPRESSURE_SPILL"
  | "WEIDA_BACKPRESSURE_COALESCE";
export type ProducerNaming = "WEIDA_PRODUCER_NAMING_FINGERPRINT" | "WEIDA_PRODUCER_NAMING_STABLE";
export type ErrorCode =
  | "WEIDA_ERROR_CODE_UNKNOWN_ENDPOINT"
  | "WEIDA_ERROR_CODE_REJECTED"
  | "WEIDA_ERROR_CODE_UNSUPPORTED"
  | "WEIDA_ERROR_CODE_INTERNAL"
  | "WEIDA_ERROR_CODE_NO_REPLY";

/** `weida::Limits`, the per-connection profile; unset fields keep weida's
 * defaults. */
export interface LimitsFields {
  maxHeaderBytes?: Count | null;
  maxConcurrentUniStreams?: Count | null;
  maxConcurrentBidiStreams?: Count | null;
  streamReceiveWindow?: Count | null;
  connectionReceiveWindow?: Count | null;
  keepAliveMs?: Count | null;
  idleTimeoutMs?: Count | null;
  helloTimeoutMs?: Count | null;
  maxSubscriptions?: Count | null;
  subscriberBufferBytes?: Count | null;
  maxSequenceScopes?: Count | null;
  maxReorderHold?: Count | null;
  maxLocalStreams?: Count | null;
  maxParkedReverse?: Count | null;
  maxDedupEntries?: Count | null;
  datagramReceiveBytes?: Count | null;
  datagramSendBytes?: Count | null;
  pathReport?: boolean | null;
  maxFlows?: Count | null;
  flowQueueBytes?: Count | null;
  flowEarlyBytes?: Count | null;
  flowEarlyHoldMs?: Count | null;
  congestion?: Congestion | null;
  headerTimeoutMs?: Count | null;
  maxHeaderStreams?: Count | null;
}

/** `weida::GuaranteeSet`, offered and required; unset dimensions are core. */
export interface GuaranteeSetFields {
  delivery?: DeliveryLevel | null;
  acknowledgement?: Acknowledgement | null;
  durability?: Durability | null;
  replicas?: Count | null;
  ordering?: OrderingMode | null;
  deduplication?: Deduplication | null;
  dedupWindowMs?: Count | null;
  backpressure?: Backpressure | null;
  producerNaming?: ProducerNaming | null;
  controlIsolated?: boolean | null;
}

/** `weida::ReconnectPolicy`; `never: true` starts from
 * `ReconnectPolicy::never()` instead of the default. */
export interface ReconnectPolicyFields {
  never?: boolean;
  initialMs?: Count | null;
  maxMs?: Count | null;
  jitter?: boolean | null;
  maxAttempts?: Count | null;
  stopOnPeerClosed?: boolean | null;
}

/** `weida::RuntimeConfig` but `resolver` and `network`. An unknown field
 * throws a TypeError. */
export interface RuntimeConfigFields {
  workerThreads?: Count | null;
  shutdownTimeoutMs?: Count | null;
  maxConnections?: Count | null;
  maxConnectionsPerPeer?: Count | null;
  endpointQueue?: Count | null;
  maxResolvedAddresses?: Count | null;
  connectAttemptTimeoutMs?: Count | null;
  sendTimeoutMs?: Count | null;
  outboxMessages?: Count | null;
  outboxBytes?: Count | null;
  outboxFull?: OutboxFull | null;
  maxHandshakes?: Count | null;
  retryAboveHandshakes?: Count | null;
  discovery?: Discovery | null;
  limits?: LimitsFields | null;
  guarantees?: GuaranteeSetFields | null;
  reconnect?: ReconnectPolicyFields | null;
}

/** Every failure of libweida (docs/c/README.md R9). */
export class WeidaError extends Error {
  /** The R9 constant name, e.g. "WEIDA_EUNTRUSTED". */
  readonly code: string;
  /** The positive status code. */
  readonly errno: number;
  /** "WEIDA_LOSS_*" for WEIDA_ECONNECTION_LOST, otherwise null. */
  readonly lossCause: string | null;
  /** The fingerprint the peer presented for WEIDA_EUNTRUSTED, otherwise null. */
  readonly fingerprint: string | null;
  /** The connection's largest payload for WEIDA_ETOO_LARGE, otherwise null. */
  readonly max: number | null;
  /** The raw OS error of WEIDA_EIO when it carried one, otherwise null. */
  readonly osCode: number | null;
}

/** The metadata of an inbound transfer or request, copied out at once. */
export interface IncomingMeta {
  readonly endpoint: string | null;
  readonly contentType: string | null;
  readonly topic: string | null;
  readonly peerFingerprint: string | null;
  /** The chain the sender presented, leaf first; null for an anonymous or
   * local sender. */
  readonly peerChain: readonly X509Certificate[] | null;
  readonly contentLen: bigint | null;
  readonly sequence: bigint | null;
  /** The sender's trace context as `traceparent` text. */
  readonly trace: string | null;
  readonly tracestate: string | null;
  /** The completion level the sender claimed for the message this answers. */
  readonly achieved: Acknowledgement | null;
}

/** A collected payload and the metadata of its transfer. */
export interface Message {
  readonly payload: Uint8Array;
  readonly meta: IncomingMeta;
}

/** The metadata an outgoing transfer carries. An unknown field throws a
 * TypeError. */
export interface TransferMeta {
  contentType?: string | null;
  contentLen?: number | bigint | null;
  /** A `traceparent`, from `newTrace()` or an inbound `IncomingMeta.trace`. */
  trace?: string | null;
  topic?: string | null;
  achieved?: Acknowledgement | null;
}

/** Every class around a C handle. A dropped or consumed object throws
 * WEIDA_ECLOSED without calling C. */
declare class Handle {
  protected constructor();
  /** Releases the handle now, ending its pending waits with WEIDA_ECLOSED. */
  drop(): void;
  [Symbol.dispose](): void;
}

export interface RuntimeConfig extends RuntimeConfigFields {}
export class RuntimeConfig {
  constructor(fields?: RuntimeConfigFields);
}

export class Runtime extends Handle {
  static owned(config?: RuntimeConfig | RuntimeConfigFields): Runtime;
  listener(): Listener;
  requester(clientTls: ClientTls): Requester;
  /** Closes every endpoint and waits for the connections to go idle.
   * Consumes the runtime. */
  shutdown(options?: WaitOptions): Promise<void>;
}

export class Trust extends Handle {
  constructor();
  /** Pins a `sha256:` fingerprint; returns this trust. */
  addPin(fingerprint: string): this;
  /** Trusts the certificate authorities in `anchor` for a peer whose chain
   * leads to one and names the dialled host; returns this trust. */
  addAnchor(anchor: X509Certificate | Bytes): this;
  /** Trusts the certificate authorities in a PEM file, read at each dial. */
  addAnchorFile(path: string): this;
}

export class Identity extends Handle {
  static generate(): Identity;
  /** A self-signed identity whose certificate names `names`. */
  static generateFor(names: string[]): Identity;
  static fromPem(cert: Bytes, key: Bytes): Identity;
  /** From Node's types or PEM; a `KeyObject` is exported as PKCS#8. */
  static from(parts: { cert: X509Certificate | X509Certificate[] | Bytes; key: KeyObject | Bytes }): Identity;
  /** One PEM file with the chain and the key, read at first use. */
  static fromPemFile(path: string): Identity;
  /** A chain file and a key file, read at first use. */
  static fromPemFiles(cert: string, key: string): Identity;
  fingerprint(): string;
  /** The certificate chain as PEM, without the key. */
  certificatePem(): string;
  /** The chain and the private key as one PEM document. Holds the private
   * key; wipe it with `fill(0)` when done. */
  toPem(): Uint8Array;
  /** The certificate chain, leaf first. */
  certificates(): X509Certificate[];
}

export class ClientTls extends Handle {
  /** Dials under `trust`, presenting `identity` when given. */
  constructor(trust: Trust, options?: { identity?: Identity });
}

export class ServerTls extends Handle {
  /** Serves as `identity`; `requireClient` requires every client to present
   * a key the trust accepts, or with "any-key" any key it proves. */
  constructor(identity: Identity, options?: { requireClient?: Trust | "any-key" });
}

export class Listener extends Handle {
  bindQuic(addr: string, serverTls: ServerTls, options?: WaitOptions): Promise<Binding>;
  bindInproc(bus: string): LocalBinding;
  replier(path: string): Replier;
}

export class Binding extends Handle {
  localAddr(): string | null;
}

export class LocalBinding extends Handle {
  bus(): string | null;
}

export class Requester extends Handle {
  connect(url: string, options?: WaitOptions): Promise<void>;
  request(body: Bytes, options?: WaitOptions): Promise<IncomingTransfer>;
  /** `request` with metadata, copied when the request starts. */
  requestWith(meta: TransferMeta | null, body: Bytes, options?: WaitOptions): Promise<IncomingTransfer>;
}

export class Replier extends Handle {
  path(): string | null;
  accept(options?: WaitOptions): Promise<IncomingRequest>;
}

export class IncomingRequest extends Handle {
  meta(): IncomingMeta;
  takeBody(): IncomingTransfer;
  /** Consumes the request. */
  reply(meta?: TransferMeta | null, options?: WaitOptions): Promise<OutgoingTransfer>;
  /** Refuses the exchange with `code`; consumes the request, an unknown name consumes nothing. */
  refuse(code: ErrorCode, options?: WaitOptions): Promise<void>;
  /** Resolves when the requester gave up; take it before `reply`, it outlives the request. */
  canceled(options?: WaitOptions): Promise<void>;
}

export class IncomingTransfer extends Handle {
  meta(): IncomingMeta;
  /** Up to `cap` bytes (default 65536); empty at the end of the payload. */
  read(cap?: number, options?: WaitOptions): Promise<Uint8Array>;
  /** Consumes the transfer. */
  collect(maxBytes: number | bigint, options?: WaitOptions): Promise<Message>;
}

export class OutgoingTransfer extends Handle {
  write(data: Bytes, options?: WaitOptions): Promise<number>;
  writeAll(data: Bytes, options?: WaitOptions): Promise<void>;
  /** The trace context propagated with this transfer; a reply inherits its
   * request's. */
  trace(): string | null;
  /** Higher is sent first among the streams of one connection; a no-op on
   * local transports. */
  setPriority(priority: number): void;
  /** A write or finish past the deadline throws WEIDA_EEXPIRED and the
   * reader sees WEIDA_ECANCELED. */
  expireAfterMs(ms: number | bigint): void;
  /** Consumes the transfer. */
  finish(): Delivery;
}

export class Delivery extends Handle {
  /** Consumes the receipt. */
  delivered(options?: WaitOptions): Promise<void>;
}
