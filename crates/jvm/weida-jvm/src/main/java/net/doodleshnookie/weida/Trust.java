package net.doodleshnookie.weida;

import java.util.Objects;

/** Whom a dialler trusts: what each dialled address names, plus pinned fingerprints. */
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
}
