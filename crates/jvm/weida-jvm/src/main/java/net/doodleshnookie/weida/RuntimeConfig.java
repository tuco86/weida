package net.doodleshnookie.weida;

import java.util.concurrent.TimeUnit;

/** {@code RuntimeConfig}: the fields set here, weida's defaults for the rest. */
public final class RuntimeConfig {
    private Integer workerThreads;
    private Long shutdownTimeoutMs;

    public RuntimeConfig() {}

    /** The reactor's worker threads; 0 is refused by {@link Runtime#owned(RuntimeConfig)}. */
    public RuntimeConfig workerThreads(int threads) {
        if (threads < 0) {
            throw new IllegalArgumentException("worker threads must not be negative: " + threads);
        }
        workerThreads = threads;
        return this;
    }

    /** The bound on {@link Runtime#shutdownAsync()}, in whole milliseconds. */
    public RuntimeConfig shutdownTimeout(long timeout, TimeUnit unit) {
        if (timeout < 0) {
            throw new IllegalArgumentException("the shutdown timeout must not be negative: " + timeout);
        }
        shutdownTimeoutMs = unit.toMillis(timeout);
        return this;
    }

    /** A C configuration holding these fields; the caller drops it. */
    long handle() {
        long cfg = Native.runtimeConfigNew();
        try {
            if (workerThreads != null) {
                Native.runtimeConfigSetWorkerThreads(cfg, workerThreads);
            }
            if (shutdownTimeoutMs != null) {
                Native.runtimeConfigSetShutdownTimeoutMs(cfg, shutdownTimeoutMs);
            }
        } catch (RuntimeException e) {
            Native.drop(Native.RUNTIME_CONFIG, cfg);
            throw e;
        }
        return cfg;
    }
}
