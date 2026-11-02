package net.doodleshnookie.weida;

import java.util.Objects;

/** A certificate chain and its private key. */
public final class Identity extends Handle {
    private Identity(long ptr) {
        super(Native.IDENTITY, ptr);
    }

    /** A self-signed identity with a fresh key, held in memory: made for pinning by fingerprint. */
    public static Identity generate() {
        return new Identity(Native.identityGenerate());
    }

    /**
     * An identity from PEM bytes: the certificate chain, leaf first, and the
     * private key. Both are copied; the material is checked when first used.
     */
    public static Identity fromPem(byte[] cert, byte[] key) {
        Objects.requireNonNull(cert, "cert");
        Objects.requireNonNull(key, "key");
        return new Identity(Native.identityFromPem(cert, key));
    }

    /** The public-key fingerprint, {@code sha256:} text. Throws {@code TLS} when the certificate does not parse. */
    public String fingerprint() {
        long id = acquire();
        try {
            return Native.identityFingerprint(id);
        } finally {
            release();
        }
    }
}
