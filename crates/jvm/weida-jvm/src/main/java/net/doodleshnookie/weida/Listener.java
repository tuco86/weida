package net.doodleshnookie.weida;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Where a runtime serves: bindings and the endpoints registered on them.
 * Closing it leaves its bindings and endpoints in place.
 */
public final class Listener extends Handle {
    Listener(long ptr) {
        super(Native.LISTENER, ptr);
    }

    /** Serves QUIC on {@code addr}, e.g. {@code "127.0.0.1:0"}, presenting {@code tls}'s identity. */
    public CompletableFuture<Binding> bindQuicAsync(final String addr, final ServerTls tls) {
        Objects.requireNonNull(addr, "addr");
        Objects.requireNonNull(tls, "tls");
        return Wait.onOp(this, Native.BIND_QUIC_OP, new Wait.OpAttempt() {
            @Override
            long start() {
                long l = acquire();
                try {
                    long t = tls.acquire();
                    try {
                        return Native.listenerBindQuic(l, addr, t);
                    } finally {
                        tls.release();
                    }
                } finally {
                    release();
                }
            }

            @Override
            Object attempt(long op) {
                long binding = Native.listenerBindQuicOpWait(op);
                return binding == 0 ? Wait.AGAIN : new Binding(binding);
            }
        });
    }

    /** {@link #bindQuicAsync}, waiting up to {@code timeout}. */
    public Binding bindQuic(String addr, ServerTls tls, long timeout, TimeUnit unit) throws InterruptedException {
        return Wait.get(bindQuicAsync(addr, tls), timeout, unit);
    }

    /** Answers on the in-process bus {@code bus}: a requester in this process reaches it at {@code weida+inproc://<bus>/<path>}. */
    public LocalBinding bindInproc(String bus) {
        Objects.requireNonNull(bus, "bus");
        long l = acquire();
        try {
            return new LocalBinding(Native.listenerBindInproc(l, bus));
        } finally {
            release();
        }
    }

    /** Registers a replier at the endpoint path {@code path}. */
    public Replier replier(String path) {
        Objects.requireNonNull(path, "path");
        long l = acquire();
        try {
            return new Replier(Native.listenerReplier(l, path));
        } finally {
            release();
        }
    }
}
