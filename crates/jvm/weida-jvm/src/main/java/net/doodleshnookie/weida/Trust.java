package net.doodleshnookie.weida;

import java.io.File;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.util.Objects;

/** Whom a dialler trusts: what each dialled address names, plus pinned fingerprints and anchors. */
public final class Trust extends Handle {
    /** A trust that accepts only what each dialled address names. */
    public Trust() {
        super(Native.TRUST, Native.trustNew());
    }

    /** Pins a fingerprint, {@code sha256:} plus 64 hex digits; returns this trust. Throws {@code INVALID_FINGERPRINT}. */
    public Trust addPin(String fingerprint) {
        Objects.requireNonNull(fingerprint, "fingerprint");
        long t = acquire();
        try {
            Native.trustAddPin(t, fingerprint);
        } finally {
            release();
        }
        return this;
    }

    /**
     * Adds the certificate authorities in PEM bytes, which are copied here and
     * parsed at each dial: a dialled host is then trusted when a chain from
     * its certificate ends at one of them and the certificate names the host.
     * Returns this trust.
     */
    public Trust addAnchor(byte[] pem) {
        Objects.requireNonNull(pem, "pem");
        long t = acquire();
        try {
            Native.trustAddAnchor(t, pem);
        } finally {
            release();
        }
        return this;
    }

    /** Adds one certificate as an anchor, as {@link #addAnchor(byte[])} does. Returns this trust. */
    public Trust addAnchor(X509Certificate cert) {
        Objects.requireNonNull(cert, "cert");
        try {
            return addAnchor(Pem.encode("CERTIFICATE", cert.getEncoded()));
        } catch (CertificateEncodingException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * Adds the certificate authorities in a PEM file, read at each dial, so
     * that a missing or bad file throws {@code TLS} then. Returns this trust.
     */
    public Trust addAnchorFile(File path) {
        Objects.requireNonNull(path, "path");
        long t = acquire();
        try {
            Native.trustAddAnchorFile(t, path.getPath());
        } finally {
            release();
        }
        return this;
    }
}
