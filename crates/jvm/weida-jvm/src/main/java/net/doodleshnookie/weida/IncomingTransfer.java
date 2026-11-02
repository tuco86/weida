package net.doodleshnookie.weida;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * An inbound payload, read like a socket or collected whole. Closed unread,
 * it refuses the payload with {@code REJECTED}.
 */
public final class IncomingTransfer extends Handle {
    /** The largest payload {@link #collectAsync} returns: the largest Java array. */
    static final long MAX_COLLECT = Integer.MAX_VALUE - 8;

    IncomingTransfer(long ptr) {
        super(Native.INCOMING_TRANSFER, ptr);
    }

    /** The transfer's metadata, copied out. */
    public IncomingMeta meta() {
        long t = acquire();
        try {
            return IncomingMeta.take(Native.incomingTransferMeta(t));
        } finally {
            release();
        }
    }

    /**
     * Reads up to {@code len} bytes into {@code buf} from {@code off}:
     * completes with the count, or -1 at the end of the payload, as
     * {@link java.io.InputStream#read(byte[], int, int)} does. A transfer the
     * peer canceled fails with {@code CANCELED}, never as an end.
     */
    public CompletableFuture<Integer> readAsync(final byte[] buf, final int off, final int len) {
        checkRange(buf, off, len);
        if (len == 0) {
            return CompletableFuture.completedFuture(0);
        }
        return Wait.onTry(this, new Wait.Attempt() {
            @Override
            Object attempt(long t) {
                int n = Native.incomingTransferRead(t, buf, off, len);
                return n < 0 ? Wait.AGAIN : Integer.valueOf(n == 0 ? -1 : n);
            }
        });
    }

    /** {@link #readAsync}, waiting up to {@code timeout}. */
    public int read(byte[] buf, int off, int len, long timeout, TimeUnit unit) throws InterruptedException {
        return Wait.get(readAsync(buf, off, len), timeout, unit);
    }

    /**
     * Reads the whole remaining payload, refusing more than {@code maxBytes}
     * (at most {@code Integer.MAX_VALUE - 8}, the largest Java array) with
     * {@code LIMIT_EXCEEDED}. Consumes the transfer.
     */
    public CompletableFuture<Message> collectAsync(long maxBytes) {
        if (maxBytes < 0) {
            throw new IllegalArgumentException("maxBytes must not be negative: " + maxBytes);
        }
        final long ceiling = Math.min(maxBytes, MAX_COLLECT);
        return Wait.onOp(null, Native.COLLECT_OP, new Wait.OpAttempt() {
            private final long[] meta = new long[1];

            @Override
            long start() {
                return Native.incomingTransferCollect(consume(), ceiling);
            }

            @Override
            Object attempt(long op) {
                byte[] payload = Native.incomingTransferCollectOpWait(op, meta);
                return payload == null ? Wait.AGAIN : new Message(payload, IncomingMeta.take(meta[0]));
            }
        });
    }

    /** {@link #collectAsync}, waiting up to {@code timeout}. */
    public Message collect(long maxBytes, long timeout, TimeUnit unit) throws InterruptedException {
        return Wait.get(collectAsync(maxBytes), timeout, unit);
    }

    static void checkRange(byte[] buf, int off, int len) {
        Objects.requireNonNull(buf, "buf");
        if (off < 0 || len < 0 || len > buf.length - off) {
            throw new IndexOutOfBoundsException("off " + off + ", len " + len + ", length " + buf.length);
        }
    }
}
