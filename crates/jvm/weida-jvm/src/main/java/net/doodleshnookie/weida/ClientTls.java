package net.doodleshnookie.weida;

import java.util.Objects;

/** The TLS terms a requester dials on: anonymous, under a {@link Trust}, which is copied. */
public final class ClientTls extends Handle {
    public ClientTls(Trust trust) {
        super(Native.CLIENT_TLS, create(trust));
    }

    private static long create(Trust trust) {
        long t = trust.acquire();
        try {
            return Native.clientTlsNew(t);
        } finally {
            trust.release();
        }
    }

    /**
     * Presents an identity to the servers dialled from now on: it is copied,
     * and a requester made afterwards uses it. Returns these terms.
     */
    public ClientTls setIdentity(Identity identity) {
        Objects.requireNonNull(identity, "identity");
        long tls = acquire();
        try {
            long id = identity.acquire();
            try {
                Native.clientTlsSetIdentity(tls, id);
            } finally {
                identity.release();
            }
        } finally {
            release();
        }
        return this;
    }
}
