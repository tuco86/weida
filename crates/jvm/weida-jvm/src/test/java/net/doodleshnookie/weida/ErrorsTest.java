package net.doodleshnookie.weida;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.EnumSet;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Status codes to exceptions, and the failures that need no descriptor. */
@Timeout(1)
class ErrorsTest {
    // docs/c/README.md R9: the 24 variants and the ABI's own five codes.
    private static final Object[][] R9 = {
        {1, "RUNTIME"}, {2, "INVALID_ADDRESS"}, {3, "INVALID_ENDPOINT_PATH"}, {4, "INVALID_FINGERPRINT"},
        {5, "ALREADY_REGISTERED"}, {6, "NOT_CONNECTED"}, {7, "CONNECTION_LOST"}, {8, "NEGOTIATION"},
        {9, "PROTOCOL"}, {10, "REJECTED"}, {11, "UNKNOWN_ENDPOINT"}, {12, "UNSUPPORTED"},
        {13, "NO_PARKED_CONNECTION"}, {14, "NO_REPLY"}, {15, "CANCELED"}, {16, "EXPIRED"},
        {17, "INDETERMINATE"}, {18, "LIMIT_EXCEEDED"}, {19, "DATAGRAMS_UNAVAILABLE"}, {20, "TOO_LARGE"},
        {21, "TLS"}, {22, "UNTRUSTED"}, {23, "IO"}, {24, "TRANSPORT"},
        {100, "AGAIN"}, {101, "INTR"}, {102, "INVAL"}, {103, "CLOSED"}, {104, "PANIC"},
    };

    @Test
    void everyStatusCodeHasItsOwnConstantAndLibweidasText() {
        Set<WeidaException.Code> seen = EnumSet.noneOf(WeidaException.Code.class);
        for (Object[] row : R9) {
            int errno = (Integer) row[0];
            WeidaException.Code code = WeidaException.Code.valueOf((String) row[1]);
            assertEquals(code, WeidaException.Code.of(errno), row[1].toString());
            WeidaException e = WeidaException.of(code);
            assertEquals(code, e.code());
            assertEquals(errno, e.errno());
            assertEquals(Weida.strerror(errno), e.getMessage());
            seen.add(code);
        }
        assertEquals(EnumSet.allOf(WeidaException.Code.class), seen);
        assertNull(WeidaException.Code.of(9999));
    }

    @Test
    void strerrorOfAnUnknownCodeIsUnknownError() {
        assertEquals("unknown error", Weida.strerror(9999));
        assertEquals("unknown error", Weida.strerror(-9999));
    }

    @Test
    void aRuntimeWithNoWorkerThreadsIsARuntimeFailure() {
        WeidaException e = assertThrows(WeidaException.class,
                () -> Runtime.owned(new RuntimeConfig().workerThreads(0)));
        assertEquals(WeidaException.Code.RUNTIME, e.code());
    }

    @Test
    void aMalformedPinIsAnInvalidFingerprint() {
        try (Trust trust = new Trust()) {
            WeidaException e = assertThrows(WeidaException.class, () -> trust.addPin("nope"));
            assertEquals(WeidaException.Code.INVALID_FINGERPRINT, e.code());
            assertEquals(4, e.errno());
        }
    }

    @Test
    void aStringHoldingANulIsAnInvalidArgument() {
        try (Trust trust = new Trust()) {
            WeidaException e = assertThrows(WeidaException.class, () -> trust.addPin("sha256:\0"));
            assertEquals(WeidaException.Code.INVAL, e.code());
        }
    }

    @Test
    void aClosedObjectThrowsClosedWithoutCallingC() {
        Trust trust = new Trust();
        trust.close();
        trust.close();
        WeidaException e = assertThrows(WeidaException.class, () -> trust.addPin("nope"));
        assertEquals(WeidaException.Code.CLOSED, e.code());
    }
}
