package net.doodleshnookie.weida;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/** The receipt of a finished outbound transfer. */
public final class Delivery extends Handle {
    Delivery(long ptr) {
        super(Native.DELIVERY, ptr);
    }

    /**
     * Waits for the peer's transport to acknowledge the whole payload; fails
     * with the refusal ({@code REJECTED}, ...) or loss that stopped it.
     * Consumes the receipt.
     */
    public CompletableFuture<Void> deliveredAsync() {
        return Wait.onOp(null, Native.DELIVERED_OP, new Wait.OpAttempt() {
            @Override
            long start() {
                return Native.deliveryDelivered(consume());
            }

            @Override
            Object attempt(long op) {
                return Native.deliveryDeliveredOpWait(op) == 0 ? Wait.AGAIN : null;
            }
        });
    }

    /** {@link #deliveredAsync}, waiting up to {@code timeout}. */
    public void delivered(long timeout, TimeUnit unit) throws InterruptedException {
        Wait.get(deliveredAsync(), timeout, unit);
    }
}
