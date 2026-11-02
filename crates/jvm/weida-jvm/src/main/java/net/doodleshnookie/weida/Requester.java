package net.doodleshnookie.weida;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/** The requesting half of Req/Rep: dials repliers and sends them requests. */
public final class Requester extends Handle {
    Requester(long ptr) {
        super(Native.REQUESTER, ptr);
    }

    /**
     * Dials {@code url}, e.g. {@code weida://sha256:...@127.0.0.1:7443/echo} or
     * {@code weida+inproc://bus/echo}; the runtime redials it when the
     * connection is lost. A peer whose key is not trusted fails with
     * {@code UNTRUSTED}.
     */
    public CompletableFuture<Void> connectAsync(final String url) {
        Objects.requireNonNull(url, "url");
        return Wait.onOp(this, Native.CONNECT_OP, new Wait.OpAttempt() {
            @Override
            long start() {
                long r = acquire();
                try {
                    return Native.requesterConnect(r, url);
                } finally {
                    release();
                }
            }

            @Override
            Object attempt(long op) {
                return Native.requesterConnectOpWait(op) == 0 ? Wait.AGAIN : null;
            }
        });
    }

    /** {@link #connectAsync}, waiting up to {@code timeout}. */
    public void connect(String url, long timeout, TimeUnit unit) throws InterruptedException {
        Wait.get(connectAsync(url), timeout, unit);
    }

    /**
     * Sends {@code body}, copied, as one request; completes with the reply as
     * soon as its header arrives. Cancelling the future abandons the exchange.
     */
    public CompletableFuture<IncomingTransfer> requestAsync(final byte[] body) {
        Objects.requireNonNull(body, "body");
        return Wait.onOp(this, Native.REQUEST_OP, new Wait.OpAttempt() {
            @Override
            long start() {
                long r = acquire();
                try {
                    return Native.requesterRequest(r, body);
                } finally {
                    release();
                }
            }

            @Override
            Object attempt(long op) {
                long transfer = Native.requesterRequestOpWait(op);
                return transfer == 0 ? Wait.AGAIN : new IncomingTransfer(transfer);
            }
        });
    }

    /** {@link #requestAsync}, waiting up to {@code timeout}. */
    public IncomingTransfer request(byte[] body, long timeout, TimeUnit unit) throws InterruptedException {
        return Wait.get(requestAsync(body), timeout, unit);
    }
}
