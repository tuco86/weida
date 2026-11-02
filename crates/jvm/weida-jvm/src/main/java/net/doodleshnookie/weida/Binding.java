package net.doodleshnookie.weida;

/** A QUIC binding: the socket a listener serves on. Closing it stops serving. */
public final class Binding extends Handle {
    Binding(long ptr) {
        super(Native.BINDING, ptr);
    }

    /** The address served on, with the port resolved, e.g. {@code "127.0.0.1:40123"}. */
    public String localAddr() {
        long b = acquire();
        try {
            return Native.bindingLocalAddr(b);
        } finally {
            release();
        }
    }
}
