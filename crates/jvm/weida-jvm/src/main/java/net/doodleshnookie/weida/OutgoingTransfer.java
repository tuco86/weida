package net.doodleshnookie.weida;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * An outbound payload, written like a socket and finished into a
 * {@link Delivery}. Closed unfinished, it is reset with {@code CANCELED}.
 */
public final class OutgoingTransfer extends Handle {
    OutgoingTransfer(long ptr) {
        super(Native.OUTGOING_TRANSFER, ptr);
    }

    /**
     * Writes up to {@code len} bytes of {@code buf} from {@code off}: completes
     * with how many flow control took. A peer that refused the payload fails
     * the write with its outcome, e.g. {@code REJECTED}.
     */
    public CompletableFuture<Integer> writeAsync(final byte[] buf, final int off, final int len) {
        IncomingTransfer.checkRange(buf, off, len);
        if (len == 0) {
            return CompletableFuture.completedFuture(0);
        }
        return Wait.onTry(this, new Wait.Attempt() {
            @Override
            Object attempt(long t) {
                int n = Native.outgoingTransferWrite(t, buf, off, len);
                return n < 0 ? Wait.AGAIN : Integer.valueOf(n);
            }
        });
    }

    /** {@link #writeAsync}, waiting up to {@code timeout}. */
    public int write(byte[] buf, int off, int len, long timeout, TimeUnit unit) throws InterruptedException {
        return Wait.get(writeAsync(buf, off, len), timeout, unit);
    }

    /** Writes all of {@code data}: the loop R3c leaves to the caller. */
    public CompletableFuture<Void> writeAllAsync(final byte[] data) {
        Objects.requireNonNull(data, "data");
        return Wait.onTry(this, new Wait.Attempt() {
            private int written;

            @Override
            Object attempt(long t) {
                while (written < data.length) {
                    int n = Native.outgoingTransferWrite(t, data, written, data.length - written);
                    if (n < 0) {
                        return Wait.AGAIN;
                    }
                    written += n;
                }
                return null;
            }
        });
    }

    /** {@link #writeAllAsync}, the timeout bounding the whole call. */
    public void writeAll(byte[] data, long timeout, TimeUnit unit) throws InterruptedException {
        Wait.get(writeAllAsync(data), timeout, unit);
    }

    /** Ends the payload and returns its delivery receipt. Consumes the transfer; fails only with {@code EXPIRED}. */
    public Delivery finish() {
        return new Delivery(Native.outgoingTransferFinish(consume()));
    }
}
