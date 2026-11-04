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

/** How many layers a segment carries: 0 to 15. */
export const MAX_SEGMENT_LAYERS: number;

/** The first cursor level an application may name. */
export const CURSOR_LEVEL_APPLICATION_FLOOR: number;

/** The `datagramReceiveBytes` a profile with flows uses. */
export const DEFAULT_DATAGRAM_RECEIVE_BYTES: number;

/** The bytes of a blob id: a BLAKE3 hash. */
export const BLOB_ID_LEN: number;

/** The most pieces a blob has. */
export const MAX_PIECES: number;

/** The smallest piece size as its base-2 logarithm: 1 KiB. */
export const PIECE_SIZE_MIN_LOG2: number;

/** The largest piece size as its base-2 logarithm: 16 MiB. */
export const PIECE_SIZE_MAX_LOG2: number;

/** The default piece size as its base-2 logarithm: 1 MiB. */
export const PIECE_SIZE_DEFAULT_LOG2: number;

/** The id of `data`: its BLAKE3 hash, `BLOB_ID_LEN` bytes. */
export function blobIdOf(data: Bytes): Uint8Array;

/** What a swarm asks a seeder about one blob. */
export type AskKind = "WEIDA_ASK_LIST" | "WEIDA_ASK_HAVE" | "WEIDA_ASK_PIECE";

/** One order of a fetch: `index` is the piece of a PIECE, 0 otherwise. */
export interface Ask {
  readonly kind: AskKind;
  readonly index: number;
}

/** One verified piece: its bytes are `Blob.span(index)` of the blob. */
export interface Piece {
  readonly index: number;
  readonly bytes: Uint8Array;
}

/** How a fetch orders; an unknown field is a `TypeError`. */
export interface FetchOptions {
  /** The piece list of an earlier fetch of the same blob, which spares the LIST order. */
  list?: PieceList | null;
  /** The fixed part of every answer's deadline. Default 30000. */
  stallMs?: Count;
  /** Bytes per second below which an answer is too slow. Default 65536; zero counts as one. */
  minRate?: Count;
  /** The most orders one seeder holds at once. Default 8; zero counts as one. */
  perPeer?: Count;
  /** Orders over all seeders at once. Default 32; zero counts as one. */
  maxInflight?: Count;
  /** The seeders a fetch takes pieces from. Default unbounded. */
  maxPeers?: Count;
  /** How long a seeder with nothing to take waits before it is asked HAVE again. Default 5000. */
  haveRefreshMs?: Count;
}

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
export type ReportMode = "WEIDA_REPORT_MODE_PROGRESS" | "WEIDA_REPORT_MODE_FINAL_ONLY";
/** A level in: a "WEIDA_ACKNOWLEDGEMENT_*" name or an application stage at
 * or above CURSOR_LEVEL_APPLICATION_FLOOR. */
export type CursorLevel = Acknowledgement | number | bigint;
/** A level out: weida's own by name, an application stage as a bigint. */
export type ReportedLevel = Acknowledgement | bigint;
/** One reported level and how far it got. */
export interface CursorEntry {
  readonly level: ReportedLevel;
  readonly offset: bigint;
}
/** The newest offset per reported level, ascending by level. */
export type CursorSet = readonly CursorEntry[];
/** What an acceptor took. */
export type IncomingKind =
  | "WEIDA_INCOMING_STREAM"
  | "WEIDA_INCOMING_EXCHANGE"
  | "WEIDA_INCOMING_SUBSCRIBED"
  | "WEIDA_INCOMING_CREDIT"
  | "WEIDA_INCOMING_UNSUBSCRIBED"
  | "WEIDA_INCOMING_FLOW";
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

/** One name lookup of a dial: the host the URL wrote, its port or null when
 * it wrote none (the answer then chooses the ports), and the most addresses
 * the answer may hold. */
export interface Lookup {
  readonly name: string;
  readonly port: number | null;
  readonly maxAddresses: number;
}

/** Answers a lookup with `"ip:port"` strings in dial order; a throw or a
 * rejection refuses it, and the dial rejects with WEIDA_EINVALID_ADDRESS. */
export type Resolver = (lookup: Lookup) => readonly string[] | Promise<readonly string[]>;

/** `weida::RuntimeConfig` but `network`. An unknown field throws a
 * TypeError. */
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
  /** Called on the JS thread for every lookup of the runtime's dials; never
   * keeps the process alive. */
  resolver?: Resolver | null;
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

/** What went missing before a transfer under per-producer ordering: the
 * number expected next and the one that arrived; `seen - expected`
 * transfers are missing. */
export interface Gap {
  readonly expected: bigint;
  readonly seen: bigint;
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
  /** The local principal of a sender over a unix socket or a named pipe;
   * null for any other sender. */
  readonly peerLocal: PeerLocal | null;
  readonly contentLen: bigint | null;
  readonly sequence: bigint | null;
  /** Null when nothing is missing or ordering is off. */
  readonly gap: Gap | null;
  /** The segment number of a RADIO stream segment. */
  readonly segment: bigint | null;
  /** The layer of a RADIO stream segment, present exactly when `segment` is. */
  readonly layer: number | null;
  /** The sender's trace context as `traceparent` text. */
  readonly trace: string | null;
  readonly tracestate: string | null;
  /** The completion level the sender claimed for the message this answers. */
  readonly achieved: Acknowledgement | null;
  /** The cursor levels the sender ordered reported, ascending; empty when
   * none. */
  readonly report: readonly ReportedLevel[];
  readonly reportMode: ReportMode;
  /** Present exactly when levels were ordered. */
  readonly reportId: bigint | null;
}

/** What a publisher dropped on one topic, by cause. */
export interface TopicDrops {
  readonly topic: string;
  readonly subscriberBudget: bigint;
  readonly subscriberQueue: bigint;
  readonly noParkedConnection: bigint;
  readonly superseded: bigint;
  readonly expired: bigint;
  readonly tooLarge: bigint;
  readonly noDatagrams: bigint;
  readonly layersCut: bigint;
}

/** What a radio dropped toward one joined dish connection, by cause. */
export interface DishDrops {
  /** The key the dish proved; null when it proved none. */
  readonly peerFingerprint: string | null;
  readonly subscriberBudget: bigint;
  readonly subscriberQueue: bigint;
  readonly superseded: bigint;
  readonly expired: bigint;
  readonly tooLarge: bigint;
  readonly noDatagrams: bigint;
  readonly layersCut: bigint;
}

/** A dish's terms for a join: its latency budget and its highest layer. */
export interface JoinTerms {
  maxAgeMs?: Count | null;
  /** 0 to 15; 16 to 255 reject the join with WEIDA_ELIMIT_EXCEEDED. */
  maxLayer?: number | null;
}

/** A segment's terms: its latency budget, its priority (an int16) and
 * whether it relays an upstream still streaming. */
export interface SegmentTerms {
  maxAgeMs?: Count | null;
  priority?: number | null;
  followsUpstream?: boolean | null;
}

/** What a dish received: a stream segment, whose topic, number and layer
 * are on `transfer.meta()`, or a datagram segment, whole. */
export type Received =
  | { readonly kind: "WEIDA_RECEIVED_SEGMENT"; readonly transfer: IncomingTransfer }
  | {
      readonly kind: "WEIDA_RECEIVED_DATAGRAM";
      readonly topic: string;
      readonly segment: bigint;
      readonly payload: Uint8Array;
    };

/** What an acceptor took: a payload handed over, or the values of a credit
 * grant or an unsubscription (`filter` null when the whole connection went
 * away). */
export type Incoming =
  | { readonly kind: "WEIDA_INCOMING_STREAM"; readonly transfer: IncomingTransfer }
  | { readonly kind: "WEIDA_INCOMING_EXCHANGE"; readonly request: IncomingRequest }
  | { readonly kind: "WEIDA_INCOMING_SUBSCRIBED"; readonly consumer: Consumer }
  | {
      readonly kind: "WEIDA_INCOMING_CREDIT";
      readonly id: bigint;
      readonly filter: string;
      readonly limit: bigint;
    }
  | {
      readonly kind: "WEIDA_INCOMING_UNSUBSCRIBED";
      readonly id: bigint;
      readonly filter: string | null;
    }
  | { readonly kind: "WEIDA_INCOMING_FLOW"; readonly flow: IncomingFlow };

/** What a sender attaches to a flow, once; a label is at most 256 bytes. */
export interface FlowMeta {
  contentType?: string | null;
  /** W3C version-00 `traceparent` text. */
  trace?: string | null;
  topic?: string | null;
}

/** What the receiver learns about a flow when it arrives. */
export interface FlowInfo {
  readonly endpoint: string;
  readonly topic: string | null;
  readonly contentType: string | null;
  readonly trace: string | null;
  readonly peerFingerprint: string | null;
  readonly peerChain: readonly X509Certificate[] | null;
  readonly peerLocal: PeerLocal | null;
}

/** The sender's counters of one flow. */
export interface FlowStats {
  readonly sent: bigint;
  readonly tooLarge: bigint;
  /** Local transports only. */
  readonly discarded: bigint;
  readonly notLive: bigint;
}

/** The receiver's counters of one flow. */
export interface IncomingFlowStats {
  readonly received: bigint;
  readonly overflow: bigint;
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
  /** The cursor levels the receiver should report; at most 16 distinct. */
  report?: readonly CursorLevel[] | null;
  reportMode?: ReportMode | null;
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
  pusher(clientTls: ClientTls): Pusher;
  /** Two subscribers on one runtime collide on the path: use one runtime per subscriber of a path. */
  subscriber(clientTls: ClientTls): Subscriber;
  /** A dialling pair: one peer, dialled with `connect`. */
  pair(clientTls: ClientTls): Paired;
  surveyor(clientTls: ClientTls): Surveyor;
  /** Two dishes on one runtime collide on the path: use one runtime per dish. */
  dish(clientTls: ClientTls): Dish;
  /** A raw peer that opens datagram flows. */
  peer(clientTls: ClientTls): Peer;
  /** A swarm: fetches one blob from every seeder it connected at once. */
  swarm(clientTls: ClientTls): Swarm;
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
  puller(path: string): Puller;
  publisher(path: string): Publisher;
  /** A bound pair, dialled by its one peer. */
  pair(path: string): Paired;
  respondent(path: string): Respondent;
  /** A bus member accepting on `path` and dialling the others on `clientTls`. */
  bus(path: string, clientTls: ClientTls): BusMember;
  /** The radio serving `path`; dropping it releases the path. */
  radio(path: string): Radio;
  /** A raw acceptor: both stream kinds and flows on `path`, tagged. */
  acceptor(path: string): Acceptor;
  /** A seeder: hands each fetch's session to the application. */
  seeder(path: string): Seeder;
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

export class Cursors extends Handle {
  /** The newest set after a change, or null once no more cursors come. */
  recv(options?: WaitOptions): Promise<CursorSet | null>;
  [Symbol.asyncIterator](): AsyncGenerator<CursorSet, void, undefined>;
}

/** Dropping it without `finish` flushes nothing. */
export class Reporter extends Handle {
  /** The levels the sender ordered, ascending. */
  levels(): readonly ReportedLevel[];
  mode(): ReportMode;
  /** Progress records at `bytes` or `intervalMs`, whichever comes first;
   * 0 and 0 write every report. */
  setGranularity(bytes: Count, intervalMs: Count): void;
  /** A level the sender did not order is ignored. */
  report(level: CursorLevel, offset: Count, options?: WaitOptions): Promise<void>;
  /** Flushes the newest offset per reported level. Consumes the reporter. */
  finish(options?: WaitOptions): Promise<void>;
}

export class Peer extends Handle {
  connect(url: string, options?: WaitOptions): Promise<void>;
  /** Whether the address was dialled. */
  disconnect(url: string): boolean;
  peerCount(): number;
  connectionStats(): readonly ConnectionStats[];
  events(): PeerEvents;
  /** The bodies the outbox discarded. */
  dropped(): bigint;
  /** A one-way stream to the next peer. */
  open(meta?: TransferMeta | null, options?: WaitOptions): Promise<OutgoingTransfer>;
  /** An exchange with the next peer. */
  openBi(meta?: TransferMeta | null, options?: WaitOptions): Promise<Exchange>;
  /** Resolves once the body is written or held in the outbox. */
  send(meta: TransferMeta | null, body: Bytes, options?: WaitOptions): Promise<void>;
  /** Segment n+1 on `topic`; it supersedes this peer's segment n there. */
  segment(topic: string, terms?: SegmentTerms | null, options?: WaitOptions): Promise<Segment>;
  segmentDrops(topic: string): TopicDrops | null;
  /** Resolves once the flow's header is written; a refusal arrives as a
   * `send` that throws. */
  openFlow(meta?: FlowMeta | null, options?: WaitOptions): Promise<Flow>;
}

export class Acceptor extends Handle {
  path(): string | null;
  accept(options?: WaitOptions): Promise<Incoming>;
}

/** A consumer that subscribed on an acceptor's path; a queue delivers to it
 * while its credit allows. */
export class Consumer extends Handle {
  /** The number of its connection, shared by that connection's subscriptions. */
  id(): bigint;
  filter(): string | null;
  path(): string | null;
  open(meta?: TransferMeta | null, options?: WaitOptions): Promise<OutgoingTransfer>;
  /** Resolves once the body is written and finished. */
  deliver(meta: TransferMeta | null, body: Bytes, options?: WaitOptions): Promise<void>;
  /** Closes the consumer's connection, ending every subscription it holds. */
  close(code: Count, reason: string): void;
}

/** `drop()` closes the flow with FIN. */
export class Flow extends Handle {
  /** Never waits; throws WEIDA_ETOO_LARGE above `maxPayload()`, and the
   * error that ended the flow once it ended. */
  send(payload: Bytes): void;
  maxPayload(): number | null;
  stats(): FlowStats;
  /** null on a local transport. */
  pathStats(): PathStats | null;
  /** Resets the flow `CANCELED`. Consumes it. */
  abort(): void;
}

/** `drop()` stops the flow `CANCELED`. */
export class IncomingFlow extends Handle {
  info(): FlowInfo;
  /** The next datagram, or null once the flow ended and was drained. */
  recv(options?: WaitOptions): Promise<Uint8Array | null>;
  [Symbol.asyncIterator](): AsyncGenerator<Uint8Array, void, undefined>;
  stats(): IncomingFlowStats;
  /** null on a local transport. */
  pathStats(): PathStats | null;
  /** Stops the flow `REJECTED`. Consumes it. */
  refuse(): void;
}

export class Replier extends Handle {
  path(): string | null;
  accept(options?: WaitOptions): Promise<IncomingRequest>;
}

export class Pusher extends Handle {
  connect(url: string, options?: WaitOptions): Promise<void>;
  /** Whether the address was dialled. */
  disconnect(url: string): boolean;
  peerCount(): number;
  connectionStats(): readonly ConnectionStats[];
  events(): PeerEvents;
  /** The bodies the outbox discarded. */
  dropped(): bigint;
  /** A one-way transfer to the next peer, whose `finish()` yields the delivery receipt. */
  open(meta?: TransferMeta | null, options?: WaitOptions): Promise<OutgoingTransfer>;
  /** Resolves once the body is written or held in the outbox. */
  send(body: Bytes, options?: WaitOptions): Promise<void>;
  sendWith(meta: TransferMeta | null, body: Bytes, options?: WaitOptions): Promise<void>;
}

export class Puller extends Handle {
  path(): string | null;
  recv(options?: WaitOptions): Promise<IncomingTransfer>;
}

export class Publisher extends Handle {
  path(): string | null;
  /** How many subscribers the payload was enqueued to; never waits on one. */
  publish(topic: string, payload: Bytes): number;
  publishWithTrace(topic: string, payload: Bytes, trace: string): number;
  open(topic: string): FanOut;
  openWithTrace(topic: string, trace: string): FanOut;
  subscriberCount(): number;
  filterCount(): number;
  dropped(): bigint;
  droppedOn(topic: string): TopicDrops | null;
  drops(): readonly TopicDrops[];
}

/** A publish in progress; dropping it without `finish()` aborts every copy. */
export class FanOut extends Handle {
  topic(): string | null;
  subscribers(): number;
  /** The subscribers left; until it settles every other call throws WEIDA_EINVAL. */
  writeWithin(chunk: Bytes, limitMs: Count, options?: WaitOptions): Promise<number>;
  writeNow(chunk: Bytes): number;
  /** The subscribers that received all of it; consumes the fan-out. */
  finish(): number;
}

export class Subscriber extends Handle {
  connect(url: string, options?: WaitOptions): Promise<void>;
  disconnect(url: string): boolean;
  peerCount(): number;
  connectionStats(): readonly ConnectionStats[];
  events(): PeerEvents;
  /** Rejects with WEIDA_EPROTOCOL for a filter outside the grammar. */
  subscribe(filter: string, options?: WaitOptions): Promise<void>;
  unsubscribe(filter: string, options?: WaitOptions): Promise<void>;
  grant(filter: string, limit: Count, options?: WaitOptions): Promise<void>;
  filterCount(): number;
  recv(options?: WaitOptions): Promise<IncomingTransfer>;
}

/** Either half of PAIR; a bound pair's first send or open waits until its peer has sent. */
export class Paired extends Handle {
  /** "" on a dialling pair. */
  path(): string | null;
  /** Once: a second address rejects with WEIDA_ELIMIT_EXCEEDED, a bound pair with WEIDA_EUNSUPPORTED. */
  connect(url: string, options?: WaitOptions): Promise<void>;
  peerCount(): number;
  connectionStats(): readonly ConnectionStats[];
  /** Null on a bound pair. */
  events(): PeerEvents | null;
  dropped(): bigint;
  open(meta?: TransferMeta | null, options?: WaitOptions): Promise<OutgoingTransfer>;
  send(body: Bytes, options?: WaitOptions): Promise<void>;
  sendWith(meta: TransferMeta | null, body: Bytes, options?: WaitOptions): Promise<void>;
  recv(options?: WaitOptions): Promise<IncomingTransfer>;
}

export class Surveyor extends Handle {
  connect(url: string, options?: WaitOptions): Promise<void>;
  peerCount(): number;
  connectionStats(): readonly ConnectionStats[];
  /** Asks every connected respondent; `deadlineMs` bounds the asking and the answers. */
  survey(body: Bytes, deadlineMs: Count, options?: WaitOptions): Promise<SurveyRun>;
  surveyWith(meta: TransferMeta | null, body: Bytes, deadlineMs: Count, options?: WaitOptions): Promise<SurveyRun>;
}

/** Dropping it resets every exchange not yet answered. */
export class SurveyRun extends Handle {
  /** The next answer, or null once the run ended; a failed answer rejects and the run goes on. */
  next(maxBytes: Count, options?: WaitOptions): Promise<Uint8Array | null>;
  /** Answers that arrived after the deadline. */
  late(): bigint;
  respondents(): number;
}

export class Respondent extends Handle {
  path(): string | null;
  accept(options?: WaitOptions): Promise<IncomingRequest>;
}

/** A blob: its root, size and piece size; WEIDA_EINVAL for a piece size out
 * of range, more than `MAX_PIECES` pieces, or an empty blob whose root is
 * not the hash of no bytes. */
export class Blob extends Handle {
  constructor(root: Uint8Array, size: Count, pieceLog2?: number);
  root(): Uint8Array;
  size(): bigint;
  pieceLog2(): number;
  count(): number;
  /** The byte range of piece `index`, end exclusive; null past the last piece. */
  span(index: number): { readonly start: bigint; readonly end: bigint } | null;
}

export class PieceList extends Handle {
  static of(data: Bytes, pieceLog2?: number): PieceList;
  /** WEIDA_EINVAL unless the bytes hash to the blob's root. */
  static parse(blob: Blob, data: Uint8Array): PieceList;
  blob(): Blob;
  /** A copy; empty for a blob of at most one piece. */
  toBytes(): Uint8Array;
  check(index: number, piece: Bytes): boolean;
}

export class TreeHasher extends Handle {
  constructor(pieceLog2?: number);
  update(data: Bytes): this;
  /** Consumes the hasher. */
  finish(): PieceList;
}

export class Bits extends Handle {
  /** `len` bits, all clear. */
  constructor(len: number);
  static full(len: number): Bits;
  /** Bit `i` is bit `i % 8`, least significant first, of byte `i / 8`; padding bits clear. */
  static fromBytes(len: number, data: Uint8Array): Bits;
  /** A copy. */
  asBytes(): Uint8Array;
  len(): number;
  get(index: number): boolean;
  set(index: number, on: boolean): this;
  ones(): number;
  all(): boolean;
}

export class Swarm extends Handle {
  connect(url: string, options?: WaitOptions): Promise<void>;
  /** Whether the address was dialled. */
  disconnect(url: string): boolean;
  peerCount(): number;
  connectionStats(): readonly ConnectionStats[];
  events(): PeerEvents;
  /** Nothing is ordered before the first `next`; WEIDA_EINVAL for a `have`
   * without one bit per piece or a list of another blob. */
  fetch(blob: Blob, have: Bits, options?: FetchOptions | null): Fetch;
}

/** Dropping it aborts every order in flight. */
export class Fetch extends Handle {
  blob(): Blob;
  /** WEIDA_EINVAL while a `next` is pending. */
  list(): PieceList | null;
  /** WEIDA_EINVAL while a `next` is pending. */
  delivered(): Bits;
  /** WEIDA_EINVAL while a `next` is pending. */
  inFlight(): Bits;
  /** The next verified piece, or null once every piece is held; rejects
   * with WEIDA_ENOT_CONNECTED when no seeder is left. */
  next(options?: WaitOptions): Promise<Piece | null>;
  /** Ends at null and leaves the fetch alive. */
  [Symbol.asyncIterator](): AsyncGenerator<Piece, void, undefined>;
}

export class Seeder extends Handle {
  path(): string | null;
  accept(options?: WaitOptions): Promise<SeedSession>;
}

/** Its calls are one at a time; WEIDA_EINVAL otherwise. */
export class SeedSession extends Handle {
  blob(): Blob;
  meta(): IncomingMeta;
  /** The number of the connection this session arrived on. */
  connection(): bigint;
  /** The next order, or null once the fetch finished its orders. */
  next(options?: WaitOptions): Promise<Ask | null>;
  answerList(list: PieceList, options?: WaitOptions): Promise<void>;
  answerHave(have: Bits, options?: WaitOptions): Promise<void>;
  answerPiece(data: Bytes, options?: WaitOptions): Promise<void>;
  /** Consumes the session. */
  decline(options?: WaitOptions): Promise<void>;
}

export class BusMember extends Handle {
  path(): string | null;
  connect(url: string, options?: WaitOptions): Promise<void>;
  peerCount(): number;
  connectionStats(): readonly ConnectionStats[];
  /** How many members the message reached. */
  send(body: Bytes, options?: WaitOptions): Promise<number>;
  sendWith(meta: TransferMeta | null, body: Bytes, options?: WaitOptions): Promise<number>;
  recv(options?: WaitOptions): Promise<IncomingTransfer>;
  /** The copies that never reached a member. */
  dropped(): bigint;
}

/** The sending half of RADIO/DISH; dropping it releases its path. */
export class Radio extends Handle {
  path(): string | null;
  /** The topic's next segment to the dishes joined now; resets the previous one's unfinished layers. */
  segment(topic: string, terms?: SegmentTerms | null): Segment;
  /** How many dishes the datagram segment was handed to. */
  datagram(topic: string, payload: Bytes): number;
  dishCount(): number;
  dropped(): bigint;
  droppedOn(topic: string): TopicDrops | null;
  drops(): readonly TopicDrops[];
  /** How many joins on `filter` of the dishes whose key is `fingerprint` went. */
  evict(fingerprint: string, filter: string): number;
  dishDrops(): readonly DishDrops[];
}

/** One open segment; dropping it without `finish()` resets every unfinished layer. */
export class Segment extends Handle {
  number(): bigint;
  topic(): string | null;
  /** How many copies took the chunk on layer 0. */
  write(chunk: Bytes): number;
  /** How many copies took the chunk; a copy capped below `layer` is skipped. */
  writeLayer(layer: number, chunk: Bytes): number;
  finishLayer(layer: number): void;
  /** How many copies still hold layer 0; consumes the segment. */
  finish(): number;
}

/** The receiving half of RADIO/DISH. */
export class Dish extends Handle {
  connect(url: string, options?: WaitOptions): Promise<void>;
  peerCount(): number;
  connectionStats(): readonly ConnectionStats[];
  events(): PeerEvents;
  /** Rejects with WEIDA_ELIMIT_EXCEEDED for a `maxLayer` of 16 or above, WEIDA_EPROTOCOL for a filter outside the grammar. */
  join(filter: string, terms?: JoinTerms | null, options?: WaitOptions): Promise<void>;
  leave(filter: string, options?: WaitOptions): Promise<void>;
  recv(options?: WaitOptions): Promise<Received>;
  /** Segments discarded because a newer one on their topic was already delivered. */
  stale(): bigint;
  /** Segments discarded because the receive queue was full. */
  overflow(): bigint;
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
  /** A new reporter for the levels the sender ordered on every call; null
   * when it ordered none. Take it before `collect`, it outlives the
   * transfer. */
  reporter(): Reporter | null;
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
  /** The queue of cursor sets the receiver reports, the first time; null
   * after that and when nothing was ordered. Take it before `finish`, it
   * outlives the transfer. */
  cursors(): Cursors | null;
  /** Consumes the transfer. */
  finish(): Delivery;
}

export class Delivery extends Handle {
  /** Consumes the receipt. */
  delivered(options?: WaitOptions): Promise<void>;
}
