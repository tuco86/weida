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
export type LossCause =
  | "WEIDA_LOSS_IDLE_TIMEOUT"
  | "WEIDA_LOSS_PEER_CLOSED"
  | "WEIDA_LOSS_PEER_RESTARTING"
  | "WEIDA_LOSS_LOCALLY_CLOSED"
  | "WEIDA_LOSS_RESET"
  | "WEIDA_LOSS_TRANSPORT_ERROR";
export type PeerEventKind =
  | "WEIDA_PEER_EVENT_CONNECTED"
  | "WEIDA_PEER_EVENT_LOST"
  | "WEIDA_PEER_EVENT_RETRYING"
  | "WEIDA_PEER_EVENT_GAVE_UP"
  | "WEIDA_PEER_EVENT_MISSED";
export type GiveUp = "WEIDA_GIVE_UP_POLICY" | "WEIDA_GIVE_UP_PEER_CHANGED" | "WEIDA_GIVE_UP_FAILED";
export type Alpn = "WEIDA_ALPN_WEIDA" | "WEIDA_ALPN_RAFT";
export type IdentityEventKind =
  | "WEIDA_IDENTITY_EVENT_LOADED"
  | "WEIDA_IDENTITY_EVENT_RENEWED"
  | "WEIDA_IDENTITY_EVENT_KEY_CHANGED"
  | "WEIDA_IDENTITY_EVENT_RENEWAL_FAILED"
  | "WEIDA_IDENTITY_EVENT_HANDOFF_STOLEN"
  | "WEIDA_IDENTITY_EVENT_TRUST_REFRESHED"
  | "WEIDA_IDENTITY_EVENT_MISSED";

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

/** The local principal the kernel proved for the sender: its uid and gid on
 * an `AF_UNIX` socket, its account SID on a named pipe. `pid` is null where
 * the platform reports none, and an observation only: never authorize on it. */
export type PeerLocal =
  | { readonly uid: number; readonly gid: number; readonly pid: number | null }
  | { readonly sid: string; readonly pid: number | null };

/** The metadata of an inbound transfer or request, copied out at once. */
export interface IncomingMeta {
  readonly endpoint: string | null;
  readonly contentType: string | null;
  readonly topic: string | null;
  readonly peerFingerprint: string | null;
  /** The chain the sender presented, leaf first; null for an anonymous or
   * local sender. */
  readonly peerChain: readonly X509Certificate[] | null;
  /** The local principal of a sender over a unix socket or a named pipe;
   * null for any other sender. */
  readonly peerLocal: PeerLocal | null;
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

/** The two halves of an exchange `Requester.open` started. */
export interface Exchange {
  readonly body: OutgoingTransfer;
  readonly reply: ReplyStream;
}

/** UDP datagrams and the bytes inside them, in one direction. */
export interface UdpCounts {
  readonly datagrams: bigint;
  readonly bytes: bigint;
}

/** The path under one QUIC connection; durations in microseconds. */
export interface PathStats {
  readonly rttUs: bigint;
  readonly minRttUs: bigint;
  readonly cwnd: bigint;
  readonly congestionEvents: bigint;
  readonly lostPackets: bigint;
  readonly lostBytes: bigint;
  readonly sentPackets: bigint;
  readonly currentMtu: bigint;
  /** null when the peer accepts no datagram. */
  readonly maxDatagramSize: bigint | null;
}

/** The transport's view of one QUIC connection. */
export interface TransportStats {
  readonly path: PathStats;
  readonly tx: UdpCounts;
  readonly rx: UdpCounts;
}

/** The peer's view of the path, from its latest record. */
export interface RemoteStats {
  readonly rttUs: bigint;
  readonly minRttUs: bigint;
  readonly cwnd: bigint;
  readonly congestionEvents: bigint;
  readonly lostPackets: bigint;
  readonly lostBytes: bigint;
  readonly sentPackets: bigint;
  readonly currentMtu: bigint;
  /** How long ago the record arrived, in microseconds. */
  readonly ageUs: bigint;
  readonly tx: UdpCounts;
  readonly rx: UdpCounts;
}

/** One live connection of a requester, labelled by the URL it dialled. */
export interface ConnectionStats {
  readonly url: string;
  readonly ageUs: bigint;
  readonly redials: bigint;
  /** null on a local transport. */
  readonly transport: TransportStats | null;
  /** null until the peer's first record, on a local transport, or with
   * `pathReport` off on either side. */
  readonly remote: RemoteStats | null;
}

/** One transition of one dialled address; a field of another kind is null. */
export interface PeerEvent {
  readonly kind: PeerEventKind;
  /** null for WEIDA_PEER_EVENT_MISSED. */
  readonly url: string | null;
  readonly peerFingerprint: string | null;
  readonly cause: LossCause | null;
  readonly attempt: number | null;
  readonly delayUs: bigint | null;
  readonly why: GiveUp | null;
  readonly whyAttempts: number | null;
  readonly whyPresented: string | null;
  readonly whyMessage: string | null;
  readonly missed: bigint | null;
}

/** One transition of an identity or trust source; a field of another kind
 * is null. */
export interface IdentityEvent {
  readonly kind: IdentityEventKind;
  /** LOADED, RENEWED: the key's fingerprint. */
  readonly fingerprint: string | null;
  /** KEY_CHANGED: the fingerprint that was. */
  readonly from: string | null;
  /** KEY_CHANGED: the fingerprint that is. */
  readonly to: string | null;
  /** RENEWAL_FAILED: why. */
  readonly message: string | null;
  /** MISSED: how many events this reader lost. */
  readonly missed: bigint | null;
}

/** How a file source watches its directory; an unknown field throws a
 * TypeError. */
export interface FilesOptions {
  /** Names the bootstrapped certificate is made for, when the directory is empty. */
  names?: string[] | null;
  /** How often the files' modification times are checked, at the next use; 0 checks at every use. */
  pollMs?: Count | null;
}

/** What one drain achieved, as counts of the transfers still
 * unacknowledged when it started. */
export interface Drained {
  /** Acknowledged by the peer's transport before the deadline. */
  readonly delivered: bigint;
  /** Unacknowledged at the deadline, refused by the peer, or lost with
   * their connection; not an error. */
  readonly outstanding: bigint;
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
  /** Duplicates discarded inside a negotiated deduplication window. */
  suppressedDuplicates(): bigint;
  /** Outgoing transfers reset because their deadline passed. */
  expiredTransfers(): bigint;
  /** Closes every endpoint and waits for the connections to go idle.
   * Consumes the runtime. */
  shutdown(options?: WaitOptions): Promise<void>;
  /** Stops admitting work, waits up to `deadlineMs` for finished
   * transfers to reach the peer's transport, then closes as `shutdown`
   * does. Consumes the runtime. */
  drain(deadlineMs: Count, options?: WaitOptions): Promise<Drained>;
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
  /** Whether the trust holds no pin and no anchor. */
  isEmpty(): boolean;
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
  /** Dials under `trust` (a `Trust` is copied, a `TrustSource` shared),
   * presenting `identity` when given, asking for `alpn`. */
  constructor(trust: Trust | TrustSource, options?: { identity?: Identity | IdentitySource; alpn?: Alpn });
}

export class ServerTls extends Handle {
  /** Serves as `identity` (an `Identity` is copied, an `IdentitySource`
   * shared); `requireClient` requires every client to present a key the
   * trust accepts, or with "any-key" any key it proves; offers `alpn`. */
  constructor(
    identity: Identity | IdentitySource,
    options?: { requireClient?: Trust | TrustSource | "any-key"; alpn?: Alpn },
  );
}

/** Where an identity comes from; shared by the TLS terms made from it. */
export class IdentitySource extends Handle {
  static fromIdentity(identity: Identity): IdentitySource;
  static ephemeral(names?: string[]): IdentitySource;
  static external(identity: Identity): IdentitySource;
  /** `cert.pem` and `key.pem` in `dir`, bootstrapped when empty, reloaded when changed. */
  static files(dir: string, options?: FilesOptions | null): IdentitySource;
  current(): Identity;
  fingerprint(): string;
  update(identity: Identity): void;
  reportFailure(reason: string): void;
  reportHandoffStolen(): void;
  /** true when the files changed and were loaded. */
  reload(): boolean;
  /** A new reader of this source's events, from now on. */
  events(): IdentityEvents;
  downgrade(): WeakIdentitySource;
}

export class WeakIdentitySource extends Handle {
  /** The source while anything holds it, else null. */
  upgrade(): IdentitySource | null;
}

/** Whom a peer is accepted as, as a source; shared by the TLS terms made from it. */
export class TrustSource extends Handle {
  static fromTrust(trust: Trust): TrustSource;
  static external(trust: Trust): TrustSource;
  /** A copy of the trust of the moment. */
  current(): Trust;
  update(trust: Trust): void;
  reportFailure(reason: string): void;
  isEmpty(): boolean;
  /** A new reader of this source's events, from now on. */
  events(): IdentityEvents;
  downgrade(): WeakTrustSource;
}

export class WeakTrustSource extends Handle {
  /** The source while anything holds it, else null. */
  upgrade(): TrustSource | null;
}

export class Listener extends Handle {
  bindQuic(addr: string, serverTls: ServerTls, options?: WaitOptions): Promise<Binding>;
  /** Serves QUIC on an already bound socket's descriptor, libweida's once the call reaches it. */
  bindQuicSocket(socket: number, serverTls: ServerTls, options?: WaitOptions): Promise<Binding>;
  /** Serves this listener's endpoints through the relay at `relay`, registered under `via`'s key. */
  bindRelay(relay: string, via: ClientTls, serverTls: ServerTls, options?: WaitOptions): Promise<Binding>;
  bindInproc(bus: string): LocalBinding;
  /** Throws WEIDA_EUNSUPPORTED on Windows. */
  bindUnix(path: string): UnixBinding;
  /** Throws WEIDA_EUNSUPPORTED off Windows. */
  bindPipe(name: string): PipeBinding;
  replier(path: string): Replier;
  /** Serves a relay on this listener's bindings. */
  relay(): Relay;
}

export class Binding extends Handle {
  localAddr(): string | null;
  /** Closes the binding's connections with SHUTDOWN; resolves once the port is free. */
  close(options?: WaitOptions): Promise<void>;
  /** Hands the socket over to a successor process. Consumes the binding. */
  handOver(options?: WaitOptions): Promise<Handoff>;
  /** Sends a predecessor's held close packets; resolves with how many were sent. */
  takeOver(held: Uint8Array, options?: WaitOptions): Promise<number>;
  /** Closes every connection from the peer with this key; how many it closed. */
  disconnect(fingerprint: string): number;
  /** A new reader of a relayed binding's registration events, or null when not relayed. */
  relayEvents(): PeerEvents | null;
}

export class Handoff extends Handle {
  /** How many close packets are held. */
  held(): number;
  /** The address the socket is bound to. */
  localAddr(): string | null;
  /** A copy of the held packets, for `Binding.takeOver`. */
  bytes(): Uint8Array;
  /** The socket's descriptor, the caller's from then on; once. */
  takeSocket(): number;
}

export class LocalBinding extends Handle {
  bus(): string | null;
}

/** Dropping it ends the binding and removes the socket file. */
export class UnixBinding extends Handle {
  path(): string | null;
}

/** Dropping it ends the binding and releases the name. */
export class PipeBinding extends Handle {
  name(): string | null;
}

export class Requester extends Handle {
  connect(url: string, options?: WaitOptions): Promise<void>;
  request(body: Bytes, options?: WaitOptions): Promise<IncomingTransfer>;
  /** `request` with metadata, copied when the request starts. */
  requestWith(meta: TransferMeta | null, body: Bytes, options?: WaitOptions): Promise<IncomingTransfer>;
  /** Opens one exchange: the request body to write and the reply stream, independent of each other. */
  open(meta?: TransferMeta | null, options?: WaitOptions): Promise<Exchange>;
  /** One frozen record per live connection, in dial order. */
  connectionStats(): readonly ConnectionStats[];
  /** A new reader of this requester's peer events, from now on. */
  events(): PeerEvents;
}

export class ReplyStream extends Handle {
  /** The reply as soon as its header arrives; consumes the stream. */
  recv(options?: WaitOptions): Promise<IncomingTransfer>;
}

export class PeerEvents extends Handle {
  /** The next event, or null once the requester, or the relayed binding's registration, is gone. */
  recv(options?: WaitOptions): Promise<PeerEvent | null>;
  [Symbol.asyncIterator](): AsyncGenerator<PeerEvent, void, undefined>;
}

export class IdentityEvents extends Handle {
  /** The next event, or null once the source is gone. */
  recv(options?: WaitOptions): Promise<IdentityEvent | null>;
  [Symbol.asyncIterator](): AsyncGenerator<IdentityEvent, void, undefined>;
}

export class Replier extends Handle {
  path(): string | null;
  accept(options?: WaitOptions): Promise<IncomingRequest>;
}

export class Relay extends Handle {
  /** The next registration or tunnel asking to be served. */
  accept(options?: WaitOptions): Promise<RelayRequest>;
}

export class RelayRequest extends Handle {
  /** The key the asker proved, or null for an anonymous dialler. */
  peerFingerprint(): string | null;
  /** null for a registration; for a tunnel, the key of the peer it reaches. */
  target(): string | null;
  /** Serves it; consumes the request. */
  admit(options?: WaitOptions): Promise<void>;
  /** Refuses it with WEIDA_EREJECTED; consumes the request. */
  refuse(options?: WaitOptions): Promise<void>;
}

export class IncomingRequest extends Handle {
  meta(): IncomingMeta;
  /** The number of the connection this request arrived on. */
  connection(): bigint;
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
  /** The number of the connection this transfer arrived on. */
  connection(): bigint;
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
