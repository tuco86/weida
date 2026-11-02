package net.doodleshnookie.weida;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * One pending wait behind a {@link CompletableFuture} (docs/c/README.md R3,
 * R4, R11): its attempt, a timeout-0 try or op wait, runs on the caller's
 * thread first; on {@code AGAIN} the watched handle's descriptor is registered
 * with the {@link Waiter}, and the attempt runs again on the waiter thread when
 * it turns readable. A result reached there completes the future through
 * {@link ForkJoinPool#commonPool()}, so no user stage runs on the waiter
 * thread. Cancelling the future, or closing the owner, unregisters the wait
 * and closes the op it waits on, which aborts the operation (R3b).
 */
final class Wait<T> implements Runnable {
    /** What an attempt returns while nothing is ready. */
    static final Object AGAIN = new Object();

    /**
     * One timeout-0 try or op wait on the pointer of its handle, taken under a
     * lease: its result, or {@link #AGAIN}. Throws {@link WeidaException} on
     * failure. An abstract class rather than a lambda: the sources compile
     * against Android API 24, which has no {@code java.lang.invoke}.
     */
    abstract static class Attempt {
        abstract Object attempt(long ptr);
    }

    /** An op (R3b): how it starts, and its timeout-0 wait. */
    abstract static class OpAttempt extends Attempt {
        /** Starts the op and returns its handle; throws {@link WeidaException}. */
        abstract long start();
    }

    private final Cancellable<T> future = new Cancellable<>(this);
    private final Handle owner;
    private final Handle watched;
    private final Attempt attempt;
    private final Handle op;
    private boolean done;
    private Waiter.Registration registration;

    private Wait(Handle owner, Handle watched, Attempt attempt, Handle op) {
        this.owner = owner;
        this.watched = watched;
        this.attempt = attempt;
        this.op = op;
    }

    /** A try on {@code handle} (a queue take, a read or a write), ended by its close. */
    static <T> CompletableFuture<T> onTry(Handle handle, Attempt attempt) {
        return new Wait<T>(handle, handle, attempt, null).begin();
    }

    /**
     * Starts an op of {@code kind} with {@code call} and waits on it; the op
     * is closed when the wait ends, which aborts it when unfinished.
     * {@code owner} is the handle whose close ends the wait, or {@code null}
     * when the start consumed the handle the op came from. A failure to start
     * completes the future exceptionally.
     */
    static <T> CompletableFuture<T> onOp(Handle owner, int kind, OpAttempt call) {
        long started;
        try {
            started = call.start();
        } catch (WeidaException e) {
            CompletableFuture<T> failed = new CompletableFuture<>();
            failed.completeExceptionally(e);
            return failed;
        }
        Op op = new Op(kind, started);
        return new Wait<T>(owner, op, call, op).begin();
    }

    /** A pending operation (R3b): only its wait sees it. */
    private static final class Op extends Handle {
        Op(int kind, long ptr) {
            super(kind, ptr);
        }
    }

    private CompletableFuture<T> begin() {
        if (owner != null && !owner.addWait(this)) {
            finish(null, WeidaException.of(WeidaException.Code.CLOSED));
        } else {
            step();
        }
        return future;
    }

    /** The waiter's action: the descriptor turned readable. */
    @Override
    public void run() {
        step();
    }

    /** Runs the attempt; on AGAIN, watches the descriptor again. */
    private void step() {
        synchronized (this) {
            if (done) {
                return;
            }
            registration = null;
        }
        Object result = null;
        Throwable error = null;
        try {
            long ptr = watched.acquire();
            try {
                result = attempt.attempt(ptr);
            } finally {
                watched.release();
            }
        } catch (RuntimeException | Error e) {
            error = e;
        }
        if (error == null && result == AGAIN) {
            synchronized (this) {
                if (done) {
                    return;
                }
                try {
                    registration = Waiter.register(watched, this);
                    return;
                } catch (RuntimeException | Error e) {
                    error = e;
                    result = null;
                }
            }
        }
        finish(result, error);
    }

    private void finish(final Object result, final Throwable error) {
        synchronized (this) {
            if (done) {
                discard(result);
                return;
            }
            done = true;
        }
        if (owner != null) {
            owner.removeWait(this);
        }
        if (op != null) {
            op.close();
        }
        if (Waiter.onWaiterThread()) {
            ForkJoinPool.commonPool().execute(new Runnable() {
                @Override
                public void run() {
                    deliver(result, error);
                }
            });
        } else {
            deliver(result, error);
        }
    }

    @SuppressWarnings("unchecked")
    private void deliver(Object result, Throwable error) {
        boolean delivered = error == null ? future.complete((T) result) : future.completeExceptionally(error);
        if (!delivered) {
            discard(result);
        }
    }

    /** A result nobody receives: a handle in it is closed at once. */
    private static void discard(Object result) {
        if (result instanceof Handle) {
            ((Handle) result).close();
        }
    }

    /**
     * Ends the wait: unregisters it, closes its op, and completes the future
     * with {@code reason} ({@code null} when the future was cancelled).
     */
    void abort(Throwable reason) {
        Waiter.Registration pending;
        synchronized (this) {
            if (done) {
                return;
            }
            done = true;
            pending = registration;
            registration = null;
        }
        if (pending != null) {
            Waiter.unregister(pending);
        }
        if (owner != null) {
            owner.removeWait(this);
        }
        if (op != null) {
            op.close();
        }
        if (reason != null) {
            future.completeExceptionally(reason);
        }
    }

    /** The future a waiting method returns: cancelling it aborts the wait. */
    private static final class Cancellable<T> extends CompletableFuture<T> {
        private final Wait<T> wait;

        Cancellable(Wait<T> wait) {
            this.wait = wait;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            boolean cancelled = super.cancel(mayInterruptIfRunning);
            if (cancelled) {
                wait.abort(null);
            }
            return cancelled;
        }
    }

    /**
     * The blocking twin of a waiting method: waits up to {@code timeout}; a
     * timeout cancels the operation and throws {@code AGAIN}, an interrupt
     * cancels it and is rethrown. A result that arrives in the same instant
     * wins over the timeout, and is closed when the interrupt wins.
     */
    static <T> T get(CompletableFuture<T> future, long timeout, TimeUnit unit) throws InterruptedException {
        try {
            return future.get(timeout, unit);
        } catch (TimeoutException e) {
            if (future.cancel(false)) {
                throw WeidaException.of(WeidaException.Code.AGAIN);
            }
            return future.join();
        } catch (InterruptedException e) {
            if (!future.cancel(false) && !future.isCompletedExceptionally()) {
                discard(future.join());
            }
            throw e;
        } catch (ExecutionException e) {
            throw unwrap(e.getCause());
        }
    }

    private static RuntimeException unwrap(Throwable cause) {
        if (cause instanceof RuntimeException) {
            return (RuntimeException) cause;
        }
        if (cause instanceof Error) {
            throw (Error) cause;
        }
        return new IllegalStateException(cause);
    }
}
