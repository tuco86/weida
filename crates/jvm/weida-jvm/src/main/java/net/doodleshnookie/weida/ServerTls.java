package net.doodleshnookie.weida;

/** The TLS terms a QUIC binding serves on: presenting an {@link Identity}, which is copied. */
public final class ServerTls extends Handle {
    public ServerTls(Identity identity) {
        super(Native.SERVER_TLS, create(identity));
    }

    private static long create(Identity identity) {
        long id = identity.acquire();
        try {
            return Native.serverTlsNew(id);
        } finally {
            identity.release();
        }
    }
}
