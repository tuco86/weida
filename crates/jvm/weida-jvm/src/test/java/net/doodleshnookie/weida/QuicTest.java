package net.doodleshnookie.weida;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Req/Rep over QUIC on localhost: the integration tier, tagged io. */
@Tag("io")
@Timeout(10)
class QuicTest {
    @Test
    void aRoundTripOverQuicWithAPinnedKey() throws Exception {
        try (Runtime runtime = Runtime.owned();
                Listener listener = runtime.listener();
                Identity identity = Identity.generate();
                ServerTls serverTls = new ServerTls(identity);
                Binding binding = listener.bindQuic("127.0.0.1:0", serverTls, InprocTest.WAIT, InprocTest.S);
                Replier replier = listener.replier("/echo");
                Trust trust = new Trust().addPin(identity.fingerprint());
                ClientTls clientTls = new ClientTls(trust);
                Requester requester = runtime.requester(clientTls)) {
            requester.connect("weida://" + identity.fingerprint() + "@" + binding.localAddr() + "/echo",
                    InprocTest.WAIT, InprocTest.S);
            CompletableFuture<String> served = InprocTest.serveAsync(replier);
            Message message = requester.request("ping".getBytes(UTF_8), InprocTest.WAIT, InprocTest.S)
                    .collect(1024, InprocTest.WAIT, InprocTest.S);
            assertEquals("gnip", new String(message.payload(), UTF_8));
            assertEquals("ping", served.get(InprocTest.WAIT, InprocTest.S));
            runtime.shutdown(InprocTest.WAIT, InprocTest.S);
        }
    }

    @Test
    void aWrongPinIsUntrustedCarryingTheFingerprint() throws Exception {
        try (Runtime runtime = Runtime.owned();
                Listener listener = runtime.listener();
                Identity identity = Identity.generate();
                Identity other = Identity.generate();
                ServerTls serverTls = new ServerTls(identity);
                Binding binding = listener.bindQuic("127.0.0.1:0", serverTls, InprocTest.WAIT, InprocTest.S);
                Trust trust = new Trust().addPin(other.fingerprint());
                ClientTls clientTls = new ClientTls(trust);
                Requester requester = runtime.requester(clientTls)) {
            WeidaException e = assertThrows(WeidaException.class, () -> {
                requester.connect("weida://" + binding.localAddr() + "/echo", InprocTest.WAIT, InprocTest.S);
                requester.request("hello".getBytes(UTF_8), InprocTest.WAIT, InprocTest.S);
            });
            assertEquals(WeidaException.Code.UNTRUSTED, e.code());
            assertEquals(identity.fingerprint(), e.fingerprint());
            runtime.shutdown(InprocTest.WAIT, InprocTest.S);
        }
    }
}
