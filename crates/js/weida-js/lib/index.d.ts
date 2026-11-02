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
}

/** A collected payload and the metadata of its transfer. */
export interface Message {
  readonly payload: Uint8Array;
  readonly meta: IncomingMeta;
}

/** The metadata an outgoing transfer carries. */
export interface TransferMeta {
  contentType?: string | null;
}

/** Every class around a C handle. A dropped or consumed object throws
 * WEIDA_ECLOSED without calling C. */
declare class Handle {
  protected constructor();
  /** Releases the handle now, ending its pending waits with WEIDA_ECLOSED. */
  drop(): void;
  [Symbol.dispose](): void;
}

export class RuntimeConfig {
  constructor(fields?: { workerThreads?: number; shutdownTimeoutMs?: number | bigint });
  workerThreads?: number;
  shutdownTimeoutMs?: number | bigint;
}

export class Runtime extends Handle {
  static owned(config?: RuntimeConfig | { workerThreads?: number; shutdownTimeoutMs?: number | bigint }): Runtime;
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
  /** Consumes the transfer. */
  finish(): Delivery;
}

export class Delivery extends Handle {
  /** Consumes the receipt. */
  delivered(options?: WaitOptions): Promise<void>;
}
