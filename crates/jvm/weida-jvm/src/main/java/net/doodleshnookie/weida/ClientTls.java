package net.doodleshnookie.weida;

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
}
