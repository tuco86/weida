package net.doodleshnookie.weida;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * One accepted request: its body and the right to reply. Closed without a
 * reply, it answers the requester with {@code NO_REPLY}.
 */
public final class IncomingRequest extends Handle {
    IncomingRequest(long ptr) {
        super(Native.INCOMING_REQUEST, ptr);
    }

    /** The request's metadata, copied out. */
    public IncomingMeta meta() {
        long q = acquire();
        try {
            return IncomingMeta.take(Native.incomingRequestMeta(q));
        } finally {
            release();
        }
    }

    /** The request body, read like a socket. Once only; a second call throws {@code CLOSED}. */
    public IncomingTransfer takeBody() {
        long q = acquire();
        try {
            return new IncomingTransfer(Native.incomingRequestTakeBody(q));
        } finally {
            release();
        }
    }

    /**
     * Opens the reply stream with {@code meta}, or empty metadata for
     * {@code null}; completes with the transfer the reply is written to.
     * Consumes the request; a body not taken is refused.
     */
    public CompletableFuture<OutgoingTransfer> replyAsync(final TransferMeta meta) {
        return Wait.onOp(null, Native.REPLY_OP, new Wait.OpAttempt() {
            @Override
            long start() {
                long handle = TransferMeta.handle(meta);
                try {
                    return Native.incomingRequestReply(consume(), handle);
                } finally {
                    if (handle != 0) {
                        Native.drop(Native.TRANSFER_META, handle);
                    }
                }
            }

            @Override
            Object attempt(long op) {
                long transfer = Native.incomingRequestReplyOpWait(op);
                return transfer == 0 ? Wait.AGAIN : new OutgoingTransfer(transfer);
            }
        });
    }

    /** {@link #replyAsync}, waiting up to {@code timeout}. */
    public OutgoingTransfer reply(TransferMeta meta, long timeout, TimeUnit unit) throws InterruptedException {
        return Wait.get(replyAsync(meta), timeout, unit);
    }
}
