package net.doodleshnookie.weida;

import java.util.ArrayList;
import java.util.List;

/**
 * The one waiter thread of the process, {@code weida-waiter}, started with
 * the first registration: it polls the readiness descriptors (R11) of every
 * pending wait plus a wake pipe with {@link Native#poll}, and runs the action
 * of each registration whose descriptor turned readable. Registrations are
 * one-shot. Each holds a lease on its handle, released only after a poll
 * round that no longer includes it, so no descriptor is closed while it is
 * polled.
 */
final class Waiter {
    private Waiter() {}

    /** One pending registration: a descriptor, the lease that keeps it open, and the action. */
    static final class Registration {
        final long fd;
        final Handle.State state;
        final Runnable action;
        boolean cancelled;

        Registration(long fd, Handle.State state, Runnable action) {
            this.fd = fd;
            this.state = state;
            this.action = action;
        }
    }

    private static final Object LOCK = new Object();
    private static final List<Registration> REGISTRATIONS = new ArrayList<>();
    private static long[] wakePipe;
    private static Thread thread;

    /** Whether the caller runs on the waiter thread. */
    static boolean onWaiterThread() {
        return Thread.currentThread() == thread;
    }

    /**
     * Runs {@code action} once on the waiter thread when {@code handle}'s
     * descriptor turns readable. Throws {@code CLOSED} when the handle is
     * closed.
     */
    static Registration register(Handle handle, Runnable action) {
        long ptr = handle.acquire();
        Registration registration;
        try {
            long fd = Native.fd(handle.state.kind, ptr);
            registration = new Registration(fd, handle.state, action);
        } catch (RuntimeException | Error e) {
            handle.release();
            throw e;
        }
        long wakeFd;
        synchronized (LOCK) {
            start();
            REGISTRATIONS.add(registration);
            wakeFd = wakePipe[1];
        }
        Native.wake(wakeFd);
        return registration;
    }

    /** Drops a registration unless it fired already; its lease goes after the current poll round. */
    static void unregister(Registration registration) {
        long wakeFd;
        synchronized (LOCK) {
            registration.cancelled = true;
            wakeFd = wakePipe[1];
        }
        Native.wake(wakeFd);
    }

    /** Under LOCK. */
    private static void start() {
        if (thread != null) {
            return;
        }
        wakePipe = Native.wakeCreate();
        thread = new Thread(new Runnable() {
            @Override
            public void run() {
                serve();
            }
        }, "weida-waiter");
        thread.setDaemon(true);
        thread.start();
    }

    private static void serve() {
        long[] fds = new long[16];
        boolean[] ready = new boolean[16];
        List<Registration> polled = new ArrayList<>();
        List<Registration> done = new ArrayList<>();
        List<Registration> fired = new ArrayList<>();
        while (true) {
            synchronized (LOCK) {
                for (int i = REGISTRATIONS.size() - 1; i >= 0; i--) {
                    Registration registration = REGISTRATIONS.get(i);
                    if (registration.cancelled) {
                        REGISTRATIONS.remove(i);
                        done.add(registration);
                    }
                }
                polled.clear();
                polled.addAll(REGISTRATIONS);
            }
            // Out of the lock: a release may drop a handle.
            for (Registration registration : done) {
                registration.state.release();
            }
            done.clear();

            int n = polled.size() + 1;
            if (n > fds.length) {
                fds = new long[n * 2];
                ready = new boolean[n * 2];
            }
            fds[0] = wakePipe[0];
            for (int i = 1; i < n; i++) {
                fds[i] = polled.get(i - 1).fd;
                ready[i] = false;
            }
            ready[0] = false;
            int rc;
            try {
                rc = Native.poll(fds, n, -1, ready);
            } catch (WeidaException e) {
                // poll(2) itself failed: back off instead of spinning; the next
                // round polls a fresh copy of the registrations.
                pause();
                continue;
            }
            if (rc < 0) {
                continue; // EINTR
            }
            if (ready[0]) {
                Native.drain(wakePipe[0]);
            }
            synchronized (LOCK) {
                for (int i = 1; i < n; i++) {
                    Registration registration = polled.get(i - 1);
                    if (ready[i] && !registration.cancelled && REGISTRATIONS.remove(registration)) {
                        fired.add(registration);
                    }
                }
            }
            for (Registration registration : fired) {
                registration.state.release();
                try {
                    registration.action.run();
                } catch (RuntimeException e) {
                    // An action completes its own future; nothing else is
                    // there to report to. The thread must keep serving.
                    Thread.UncaughtExceptionHandler handler = Thread.getDefaultUncaughtExceptionHandler();
                    if (handler != null) {
                        handler.uncaughtException(Thread.currentThread(), e);
                    }
                }
            }
            fired.clear();
        }
    }

    private static void pause() {
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            // The waiter is never interrupted on purpose; keep serving.
        }
    }
}
