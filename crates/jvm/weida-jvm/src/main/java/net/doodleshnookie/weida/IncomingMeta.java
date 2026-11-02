package net.doodleshnookie.weida;

import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Objects;
import java.util.OptionalLong;

/** The metadata of an inbound transfer or request: an immutable value, copied out of libweida at once. */
public final class IncomingMeta {
    private final String endpoint;
    private final String contentType;
    private final String topic;
    private final String peerFingerprint;
    private final OptionalLong contentLen;
    private final OptionalLong sequence;
    private final byte[][] peerChain;

    private IncomingMeta(String endpoint, String contentType, String topic, String peerFingerprint,
            OptionalLong contentLen, OptionalLong sequence, byte[][] peerChain) {
        this.endpoint = endpoint;
        this.contentType = contentType;
        this.topic = topic;
        this.peerFingerprint = peerFingerprint;
        this.contentLen = contentLen;
        this.sequence = sequence;
        this.peerChain = peerChain;
    }

    /** Copies a {@code weida_incoming_meta} out and drops it. */
    static IncomingMeta take(long meta) {
        try {
            long[] numbers = new long[2];
            int present = Native.incomingMetaNumbers(meta, numbers);
            return new IncomingMeta(
                    Native.incomingMetaString(meta, 0),
                    Native.incomingMetaString(meta, 1),
                    Native.incomingMetaString(meta, 2),
                    Native.incomingMetaString(meta, 3),
                    (present & 1) != 0 ? OptionalLong.of(numbers[0]) : OptionalLong.empty(),
                    (present & 2) != 0 ? OptionalLong.of(numbers[1]) : OptionalLong.empty(),
                    Native.incomingMetaPeerChain(meta));
        } finally {
            Native.drop(Native.INCOMING_META, meta);
        }
    }

    /** The endpoint path the transfer was sent to; {@code null} on a reply. */
    public String endpoint() {
        return endpoint;
    }

    /** The opaque content type label; {@code null} when the sender set none. */
    public String contentType() {
        return contentType;
    }

    /** The Pub/Sub topic; {@code null} when the transfer has none. */
    public String topic() {
        return topic;
    }

    /** The fingerprint the sender proved in the TLS handshake; {@code null} when it dialled anonymously or locally. */
    public String peerFingerprint() {
        return peerFingerprint;
    }

    /**
     * The certificates the peer presented, leaf first; {@code null} for an
     * anonymous or local peer, or a chain over the bound weida keeps. A new
     * array, parsed on each call; throws {@code TLS} when one does not parse.
     */
    public X509Certificate[] peerChain() {
        if (peerChain == null) {
            return null;
        }
        X509Certificate[] out = new X509Certificate[peerChain.length];
        for (int i = 0; i < out.length; i++) {
            out[i] = Pem.certificate(peerChain[i], i);
        }
        return out;
    }

    /** The advisory payload length the sender stated. An unsigned 64-bit value. */
    public OptionalLong contentLen() {
        return contentLen;
    }

    /** The sender's per-producer sequence number. An unsigned 64-bit value. */
    public OptionalLong sequence() {
        return sequence;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof IncomingMeta)) {
            return false;
        }
        IncomingMeta that = (IncomingMeta) other;
        return Objects.equals(endpoint, that.endpoint)
                && Objects.equals(contentType, that.contentType)
                && Objects.equals(topic, that.topic)
                && Objects.equals(peerFingerprint, that.peerFingerprint)
                && contentLen.equals(that.contentLen)
                && sequence.equals(that.sequence)
                && Arrays.deepEquals(peerChain, that.peerChain);
    }

    @Override
    public int hashCode() {
        return 31 * Objects.hash(endpoint, contentType, topic, peerFingerprint, contentLen, sequence)
                + Arrays.deepHashCode(peerChain);
    }

    @Override
    public String toString() {
        return "IncomingMeta{endpoint=" + endpoint + ", contentType=" + contentType + ", topic=" + topic
                + ", peerFingerprint=" + peerFingerprint + ", contentLen=" + contentLen + ", sequence=" + sequence
                + ", peerChain=" + (peerChain == null ? "null" : peerChain.length + " certificates") + "}";
    }
}
