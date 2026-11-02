package net.doodleshnookie.weida;

import java.util.OptionalInt;
import java.util.OptionalLong;

/**
 * A failed call of libweida (docs/c/README.md R9): its status code, the
 * library's text for this failure, and the thread-local record the JNI glue
 * read in the same native call that failed.
 */
public final class WeidaException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    /** One constant per status code of the C ABI, in R9's order. */
    public enum Code {
        RUNTIME(1),
        INVALID_ADDRESS(2),
        INVALID_ENDPOINT_PATH(3),
        INVALID_FINGERPRINT(4),
        ALREADY_REGISTERED(5),
        NOT_CONNECTED(6),
        CONNECTION_LOST(7),
        NEGOTIATION(8),
        PROTOCOL(9),
        REJECTED(10),
        UNKNOWN_ENDPOINT(11),
        UNSUPPORTED(12),
        NO_PARKED_CONNECTION(13),
        NO_REPLY(14),
        CANCELED(15),
        EXPIRED(16),
        INDETERMINATE(17),
        LIMIT_EXCEEDED(18),
        DATAGRAMS_UNAVAILABLE(19),
        TOO_LARGE(20),
        TLS(21),
        UNTRUSTED(22),
        IO(23),
        TRANSPORT(24),
        /** The wait gave up: nothing was ready within the timeout. */
        AGAIN(100),
        /** A signal reached the waiting thread. */
        INTR(101),
        /** A NULL, an invalid string or an out-of-range argument. */
        INVAL(102),
        /** The object was closed, spent or consumed. */
        CLOSED(103),
        /** The library caught a panic. */
        PANIC(104);

        private final int errno;

        Code(int errno) {
            this.errno = errno;
        }

        /** The positive {@code WEIDA_E*} value. */
        public int errno() {
            return errno;
        }

        /** The constant of a positive {@code WEIDA_E*} value, or {@code null} for one this binding does not know. */
        public static Code of(int errno) {
            for (Code code : values()) {
                if (code.errno == errno) {
                    return code;
                }
            }
            return null;
        }
    }

    /** Why a connection was lost: the {@code WEIDA_LOSS_*} constants. */
    public enum LossCause {
        IDLE_TIMEOUT,
        PEER_CLOSED,
        PEER_RESTARTING,
        LOCALLY_CLOSED,
        RESET,
        TRANSPORT_ERROR;

        static LossCause of(int value) {
            LossCause[] all = values();
            return value >= 1 && value <= all.length ? all[value - 1] : null;
        }
    }

    private final int errno;
    private final LossCause lossCause;
    private final String fingerprint;
    private final long max;
    private final int osCode;

    /** Called by the JNI glue; the signature is cached in its JNI_OnLoad. */
    WeidaException(int errno, String message, int lossCause, String fingerprint, long max, int osCode) {
        super(message);
        this.errno = errno;
        this.lossCause = LossCause.of(lossCause);
        this.fingerprint = fingerprint;
        this.max = max;
        this.osCode = osCode;
    }

    /** A failure no C call reported, so without a record: its text is libweida's. */
    static WeidaException of(Code code) {
        return new WeidaException(code.errno(), Native.strerror(code.errno()), 0, null, -1, 0);
    }

    /** The code, or {@code null} for a value this binding does not know ({@link #errno()} has it). */
    public Code code() {
        return Code.of(errno);
    }

    /** The positive {@code WEIDA_E*} value. */
    public int errno() {
        return errno;
    }

    /** Why the connection was lost, for {@link Code#CONNECTION_LOST}; otherwise {@code null}. */
    public LossCause lossCause() {
        return lossCause;
    }

    /** The fingerprint the peer presented, for {@link Code#UNTRUSTED}; otherwise {@code null}. */
    public String fingerprint() {
        return fingerprint;
    }

    /** The largest payload the connection carries, for {@link Code#TOO_LARGE}. */
    public OptionalLong max() {
        return max >= 0 ? OptionalLong.of(max) : OptionalLong.empty();
    }

    /** The raw OS error, for {@link Code#IO} when there was one. */
    public OptionalInt osCode() {
        return osCode != 0 ? OptionalInt.of(osCode) : OptionalInt.empty();
    }
}
