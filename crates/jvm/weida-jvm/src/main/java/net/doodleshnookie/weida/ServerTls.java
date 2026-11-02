package net.doodleshnookie.weida;

import java.util.Objects;

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

    /**
     * Refuses every client whose key the trust does not accept; the trust is
     * copied. A refused client gets {@code TLS} on its side and nothing reaches the
     * replier. An empty trust makes the bind throw {@code TLS}. The last call wins.
     * Returns these terms.
     */
    public ServerTls requireClient(Trust trust) {
        Objects.requireNonNull(trust, "trust");
        long tls = acquire();
        try {
            long t = trust.acquire();
            try {
                Native.serverTlsRequireClient(tls, t);
            } finally {
                trust.release();
            }
        } finally {
            release();
        }
        return this;
    }

    /**
     * Requires every client to present a key and prove it holds it, whatever
     * the key; the server then reads it from the metadata of a request. A
     * refused client gets {@code TLS} on its side and nothing reaches the
     * replier. The last call wins. Returns these terms.
     */
    public ServerTls requireAnyKey() {
        long tls = acquire();
        try {
            Native.serverTlsRequireAnyKey(tls);
        } finally {
            release();
        }
        return this;
    }
}
