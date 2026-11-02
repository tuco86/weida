// Declarations of lib/index.js (docs/libraries/weida-js.md).

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
}

export class Identity extends Handle {
  static generate(): Identity;
  static fromPem(cert: Bytes, key: Bytes): Identity;
  fingerprint(): string;
}

export class ClientTls extends Handle {
  constructor(trust: Trust);
}

export class ServerTls extends Handle {
  constructor(identity: Identity);
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
