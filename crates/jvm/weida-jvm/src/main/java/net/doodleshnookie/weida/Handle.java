package net.doodleshnookie.weida;

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * One C handle (docs/c/README.md R5, R6). Every call takes a lease, reads the
 * pointer, calls the native and releases the lease. {@link #close()} and every
 * consuming method retire the pointer at once, so no call starts afterwards;
 * the C drop runs when the last lease is released, so no call is still on
 * its way into C when it does. Pending waits of a closed or consumed handle
 * complete exceptionally with {@link WeidaException.Code#CLOSED}. A handle
 * never closed is dropped by the {@code weida-cleaner} thread once the
 * garbage collector finds it unreachable.
 */
abstract class Handle implements AutoCloseable {
    final State state;
    private final Set<Wait<?>> waits = Collections.newSetFromMap(new IdentityHashMap<Wait<?>, Boolean>());

    Handle(int kind, long ptr) {
        state = new State(kind, ptr);
        Reaper.track(this, state);
    }

    /**
     * The pointer under a lease; pair with {@link #release()} in a
     * {@code finally}. Throws {@code CLOSED} once the handle is closed or
     * consumed.
     */
    final long acquire() {
        return state.acquire();
    }

    final void release() {
        state.release();
    }

    /**
     * Retires the pointer for a C function that consumes it (R6): ends the
     * pending waits, waits for the leases of calls in progress, and returns
     * the pointer, which no other call will use.
     */
    final long consume() {
        long ptr = state.retire();
        cancelWaits();
        state.awaitNoLeases();
        return ptr;
    }

    /**
     * Releases the handle, with Rust's {@code Drop} effects (R1): an unanswered
     * request answers {@code NO_REPLY}, an unfinished outgoing transfer is
     * reset {@code CANCELED}, an unread incoming one is refused
     * {@code REJECTED}. Pending waits complete with {@code CLOSED}. A second
     * close does nothing.
     */
    @Override
    public void close() {
        if (state.close()) {
            cancelWaits();
        }
    }

    /** Registers a pending wait that this handle's close ends; false when closed. */
    final boolean addWait(Wait<?> wait) {
        synchronized (waits) {
            if (state.isClosed()) {
                return false;
            }
            waits.add(wait);
            return true;
        }
    }

    final void removeWait(Wait<?> wait) {
        synchronized (waits) {
            waits.remove(wait);
        }
    }

    private void cancelWaits() {
        List<Wait<?>> pending;
        synchronized (waits) {
            pending = new ArrayList<>(waits);
            waits.clear();
        }
        for (Wait<?> wait : pending) {
            wait.abort(WeidaException.of(WeidaException.Code.CLOSED));
        }
    }

    /** The pointer and its leases, apart from the handle so that the cleaner can drop it. */
    static final class State {
        final int kind;
        private long ptr;
        private int leases;
        private boolean closed;
        private Reaper reaper;

        State(int kind, long ptr) {
            this.kind = kind;
            this.ptr = ptr;
        }

        synchronized boolean isClosed() {
            return closed;
        }

        synchronized long acquire() {
            if (closed) {
                throw WeidaException.of(WeidaException.Code.CLOSED);
            }
            leases++;
            return ptr;
        }

        void release() {
            long drop;
            synchronized (this) {
                leases--;
                if (leases > 0) {
                    return;
                }
                notifyAll();
                if (!closed || ptr == 0) {
                    return;
                }
                drop = ptr;
                ptr = 0;
            }
            Native.drop(kind, drop);
        }

        /**
         * Marks the handle consumed and takes its pointer, so that no release
         * drops it; throws {@code CLOSED} when it already was closed.
         */
        synchronized long retire() {
            if (closed) {
                throw WeidaException.of(WeidaException.Code.CLOSED);
            }
            closed = true;
            untrack();
            long taken = ptr;
            ptr = 0;
            return taken;
        }

        /**
         * Waits until no lease is held: calls in progress are non-blocking,
         * and the waiter releases a cancelled registration's lease right after
         * its poll round.
         */
        void awaitNoLeases() {
            boolean interrupted = false;
            synchronized (this) {
                while (leases > 0) {
                    try {
                        wait();
                    } catch (InterruptedException e) {
                        interrupted = true;
                    }
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }

        /** Closes; the C drop runs now or at the last release. False when already closed. */
        boolean close() {
            long drop;
            synchronized (this) {
                if (closed) {
                    return false;
                }
                closed = true;
                untrack();
                if (leases > 0 || ptr == 0) {
                    return true;
                }
                drop = ptr;
                ptr = 0;
            }
            Native.drop(kind, drop);
            return true;
        }

        /** Under the lock. */
        private void untrack() {
            if (reaper != null) {
                Reaper.untrack(reaper);
                reaper = null;
            }
        }
    }

    /** The garbage collector's backstop: drops the state of a handle that was never closed. */
    static final class Reaper extends PhantomReference<Handle> {
        private static final ReferenceQueue<Handle> QUEUE = new ReferenceQueue<>();
        private static final Set<Reaper> LIVE = Collections.newSetFromMap(new ConcurrentHashMap<Reaper, Boolean>());

        static {
            // Started once, when the first handle is made.
            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    reap();
                }
            }, "weida-cleaner");
            thread.setDaemon(true);
            thread.start();
        }

        private final State state;

        private Reaper(Handle handle, State state) {
            super(handle, QUEUE);
            this.state = state;
        }

        static void track(Handle handle, State state) {
            Reaper reaper = new Reaper(handle, state);
            LIVE.add(reaper);
            synchronized (state) {
                state.reaper = reaper;
            }
        }

        static void untrack(Reaper reaper) {
            LIVE.remove(reaper);
            reaper.clear();
        }

        private static void reap() {
            while (true) {
                Reaper reaper;
                try {
                    reaper = (Reaper) QUEUE.remove();
                } catch (InterruptedException e) {
                    continue;
                }
                LIVE.remove(reaper);
                reaper.state.close();
            }
        }
    }
}
