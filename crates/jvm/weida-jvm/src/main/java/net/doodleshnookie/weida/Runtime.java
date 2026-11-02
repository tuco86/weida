package net.doodleshnookie.weida;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * A weida runtime on a reactor of its own, and every endpoint made from it.
 * {@link #close()} releases this reference only: the reactor shuts down in the
 * background once everything made from it is closed too (R13).
 *
 * <p>The name shadows {@code java.lang.Runtime}: a file that imports
 * {@code net.doodleshnookie.weida.*} imports this class by name as well.
 */
public final class Runtime extends Handle {
    private Runtime(long ptr) {
        super(Native.RUNTIME, ptr);
    }

    /** A runtime with weida's defaults. */
    public static Runtime owned() {
        return new Runtime(Native.runtimeOwned(0));
    }

    /** A runtime with {@code config}'s fields. Throws {@code RUNTIME} for zero worker threads. */
    public static Runtime owned(RuntimeConfig config) {
        long cfg = config.handle();
        try {
            return new Runtime(Native.runtimeOwned(cfg));
        } finally {
            Native.drop(Native.RUNTIME_CONFIG, cfg);
        }
    }

    /** The runtime's listener: where it serves. */
    public Listener listener() {
        long rt = acquire();
        try {
            return new Listener(Native.runtimeListener(rt));
        } finally {
            release();
        }
    }

    /** A requester that dials on {@code tls}'s terms. */
    public Requester requester(ClientTls tls) {
        Objects.requireNonNull(tls, "tls");
        long rt = acquire();
        try {
            long t = tls.acquire();
            try {
                return new Requester(Native.runtimeRequester(rt, t));
            } finally {
                tls.release();
            }
        } finally {
            release();
        }
    }

    /**
     * Closes every endpoint and waits, bounded by the configured shutdown
     * timeout, for the connections to go idle. Consumes the runtime.
     */
    public CompletableFuture<Void> shutdownAsync() {
        return Wait.onOp(null, Native.SHUTDOWN_OP, new Wait.OpAttempt() {
            @Override
            long start() {
                return Native.runtimeShutdown(consume());
            }

            @Override
            Object attempt(long op) {
                return Native.runtimeShutdownOpWait(op) == 0 ? Wait.AGAIN : null;
            }
        });
    }

    /** {@link #shutdownAsync()}, waiting up to {@code timeout}. */
    public void shutdown(long timeout, TimeUnit unit) throws InterruptedException {
        Wait.get(shutdownAsync(), timeout, unit);
    }
}
