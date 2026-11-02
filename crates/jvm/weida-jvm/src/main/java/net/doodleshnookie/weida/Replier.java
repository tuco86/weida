package net.doodleshnookie.weida;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * The replying half of Req/Rep, serving one endpoint path. Closing it
 * unregisters the path and completes its pending accepts with {@code CLOSED}.
 */
public final class Replier extends Handle {
    private static final Wait.Attempt ACCEPT = new Wait.Attempt() {
        @Override
        Object attempt(long replier) {
            long request = Native.replierAccept(replier);
            return request == 0 ? Wait.AGAIN : new IncomingRequest(request);
        }
    };

    Replier(long ptr) {
        super(Native.REPLIER, ptr);
    }

    /** The endpoint path this replier serves. */
    public String path() {
        long r = acquire();
        try {
            return Native.replierPath(r);
        } finally {
            release();
        }
    }

    /**
     * The next request, as soon as its header arrives; its body is read from
     * {@link IncomingRequest#takeBody()}. Cancelling the future loses nothing:
     * the in-flight take stays in the replier for the next accept.
     */
    public CompletableFuture<IncomingRequest> acceptAsync() {
        return Wait.onTry(this, ACCEPT);
    }

    /** {@link #acceptAsync}, waiting up to {@code timeout}. */
    public IncomingRequest accept(long timeout, TimeUnit unit) throws InterruptedException {
        return Wait.get(acceptAsync(), timeout, unit);
    }
}
