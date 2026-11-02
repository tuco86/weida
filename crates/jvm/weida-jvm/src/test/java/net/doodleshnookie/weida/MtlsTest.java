package net.doodleshnookie.weida;

import static java.nio.charset.StandardCharsets.UTF_8;
import static net.doodleshnookie.weida.InprocTest.S;
import static net.doodleshnookie.weida.InprocTest.WAIT;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/** Client keys, trust anchors and the presented chain over QUIC on localhost: the integration tier, tagged io. */
@Tag("io")
@Timeout(10)
class MtlsTest {
    /** Answers one request with its body reversed, like {@link InprocTest#serveAsync}; completes with the request's metadata. */
    static CompletableFuture<IncomingMeta> serveSeeing(Replier replier) {
        return replier.acceptAsync().thenCompose(request -> {
            IncomingMeta meta = request.meta();
            return request.takeBody().collectAsync(1024)
                    .thenCompose(body -> request.replyAsync(null)
                            .thenCompose(reply -> reply.writeAllAsync(InprocTest.reversed(body.payload()))
                                    .thenCompose(done -> reply.finish().deliveredAsync())))
                    .thenApply(done -> meta);
        });
    }

    /** The failure of dialling {@code url} and sending a request. */
    static WeidaException refused(Requester requester, String url) {
        return assertThrows(WeidaException.class, () -> {
            requester.connect(url, WAIT, S);
            requester.request("hello".getBytes(UTF_8), WAIT, S);
        });
    }

    /** Connects, sends {@code ping}, checks the reversed answer and returns what the server saw. */
    static IncomingMeta roundTrip(Requester requester, String url, Replier replier) throws Exception {
        CompletableFuture<IncomingMeta> seen = serveSeeing(replier);
        requester.connect(url, WAIT, S);
        Message message = requester.request("ping".getBytes(UTF_8), WAIT, S).collect(1024, WAIT, S);
        assertEquals("gnip", new String(message.payload(), UTF_8));
        return seen.get(WAIT, S);
    }

    private static String urlOf(Binding binding) {
        return "weida://" + binding.localAddr() + "/echo";
    }

    @Test
    void aPinnedClientKeyIsAcceptedAndTheServerSeesItsKeyAndChain() throws Exception {
        try (Runtime runtime = Runtime.owned();
                Listener listener = runtime.listener();
                Identity server = Identity.generate();
                Identity client = Identity.generate();
                Trust clients = new Trust().addPin(client.fingerprint());
                ServerTls serverTls = new ServerTls(server).requireClient(clients);
                Binding binding = listener.bindQuic("127.0.0.1:0", serverTls, WAIT, S);
                Replier replier = listener.replier("/echo");
                Trust servers = new Trust().addPin(server.fingerprint());
                ClientTls clientTls = new ClientTls(servers).setIdentity(client);
                Requester requester = runtime.requester(clientTls)) {
            IncomingMeta seen = roundTrip(requester, urlOf(binding), replier);
            assertEquals(client.fingerprint(), seen.peerFingerprint());
            assertEquals(client.certificates()[0], seen.peerChain()[0]);
            runtime.shutdown(WAIT, S);
        }
    }

    @Test
    void anAnonymousOrUnknownClientIsRefusedWithTlsAndNothingReachesTheServer() throws Exception {
        try (Runtime runtime = Runtime.owned();
                Listener listener = runtime.listener();
                Identity server = Identity.generate();
                Identity client = Identity.generate();
                Identity stranger = Identity.generate();
                Trust clients = new Trust().addPin(client.fingerprint());
                ServerTls serverTls = new ServerTls(server).requireClient(clients);
                Binding binding = listener.bindQuic("127.0.0.1:0", serverTls, WAIT, S);
                Replier replier = listener.replier("/echo");
                Trust servers = new Trust().addPin(server.fingerprint());
                ClientTls anonymous = new ClientTls(servers);
                ClientTls unknown = new ClientTls(servers).setIdentity(stranger);
                Requester anonymousRequester = runtime.requester(anonymous);
                Requester unknownRequester = runtime.requester(unknown)) {
            assertEquals(WeidaException.Code.TLS, refused(anonymousRequester, urlOf(binding)).code());
            assertEquals(WeidaException.Code.TLS, refused(unknownRequester, urlOf(binding)).code());
            WeidaException e = assertThrows(WeidaException.class, () -> replier.accept(200, TimeUnit.MILLISECONDS));
            assertEquals(WeidaException.Code.AGAIN, e.code());
            runtime.shutdown(WAIT, S);
        }
    }

    @Test
    void aBindingThatRequiresClientsFromAnEmptyTrustDoesNotBind() throws Exception {
        try (Runtime runtime = Runtime.owned();
                Listener listener = runtime.listener();
                Identity server = Identity.generate();
                Trust nobody = new Trust();
                ServerTls serverTls = new ServerTls(server).requireClient(nobody)) {
            WeidaException e = assertThrows(WeidaException.class,
                    () -> listener.bindQuic("127.0.0.1:0", serverTls, WAIT, S));
            assertEquals(WeidaException.Code.TLS, e.code());
            assertTrue(e.getMessage().contains("client trust is empty"), e.getMessage());
            runtime.shutdown(WAIT, S);
        }
    }

    @Test
    void anyKeyExposesTheFingerprintAndTheChain() throws Exception {
        try (Runtime runtime = Runtime.owned();
                Listener listener = runtime.listener();
                Identity server = Identity.generate();
                Identity client = Identity.generate();
                ServerTls serverTls = new ServerTls(server).requireAnyKey();
                Binding binding = listener.bindQuic("127.0.0.1:0", serverTls, WAIT, S);
                Replier replier = listener.replier("/echo");
                Trust servers = new Trust().addPin(server.fingerprint());
                ClientTls anonymous = new ClientTls(servers);
                ClientTls presenting = new ClientTls(servers).setIdentity(client);
                Requester anonymousRequester = runtime.requester(anonymous);
                Requester presentingRequester = runtime.requester(presenting)) {
            assertEquals(WeidaException.Code.TLS, refused(anonymousRequester, urlOf(binding)).code());
            IncomingMeta seen = roundTrip(presentingRequester, urlOf(binding), replier);
            assertEquals(1, seen.peerChain().length);
            assertEquals(client.fingerprint(), seen.peerFingerprint());
            runtime.shutdown(WAIT, S);
        }
    }

    @Test
    void anAnchorAcceptsTheDialledNameAndRefusesAnother() throws Exception {
        try (Runtime runtime = Runtime.owned();
                Listener listener = runtime.listener();
                Identity server = Identity.generateFor("127.0.0.1");
                ServerTls serverTls = new ServerTls(server);
                Binding binding = listener.bindQuic("127.0.0.1:0", serverTls, WAIT, S);
                Replier replier = listener.replier("/echo");
                Trust anchored = new Trust().addAnchor(server.certificates()[0]);
                ClientTls clientTls = new ClientTls(anchored);
                Requester requester = runtime.requester(clientTls);
                Identity other = Identity.generateFor("localhost");
                ServerTls otherTls = new ServerTls(other);
                Binding otherBinding = listener.bindQuic("127.0.0.1:0", otherTls, WAIT, S);
                Trust otherAnchored = new Trust().addAnchor(other.certificates()[0]);
                ClientTls otherClientTls = new ClientTls(otherAnchored);
                Requester otherRequester = runtime.requester(otherClientTls)) {
            IncomingMeta seen = roundTrip(requester, urlOf(binding), replier);
            assertNull(seen.peerChain());
            WeidaException e = refused(otherRequester, urlOf(otherBinding));
            assertEquals(WeidaException.Code.UNTRUSTED, e.code());
            assertEquals(other.fingerprint(), e.fingerprint());
            runtime.shutdown(WAIT, S);
        }
    }

    @Test
    void identityAndAnchorFilesReadBack(@TempDir Path dir) throws Exception {
        try (Runtime runtime = Runtime.owned();
                Listener listener = runtime.listener();
                Identity generated = Identity.generateFor("127.0.0.1")) {
            Path idPem = dir.resolve("id.pem");
            Path certPem = dir.resolve("cert.pem");
            byte[] withKey = generated.toPem();
            Files.write(idPem, withKey);
            Arrays.fill(withKey, (byte) 0);
            Files.write(certPem, generated.certificatePem());
            try (Identity fromOne = Identity.fromPemFile(idPem.toFile());
                    Identity fromTwo = Identity.fromPemFiles(certPem.toFile(), idPem.toFile());
                    Identity missing = Identity.fromPemFile(dir.resolve("missing.pem").toFile())) {
                assertEquals(generated.fingerprint(), fromOne.fingerprint());
                assertEquals(generated.fingerprint(), fromTwo.fingerprint());
                assertEquals(WeidaException.Code.TLS, assertThrows(WeidaException.class, missing::fingerprint).code());
                try (ServerTls serverTls = new ServerTls(fromOne);
                        Binding binding = listener.bindQuic("127.0.0.1:0", serverTls, WAIT, S);
                        Replier replier = listener.replier("/echo");
                        Trust anchored = new Trust().addAnchorFile(certPem.toFile());
                        ClientTls clientTls = new ClientTls(anchored);
                        Requester requester = runtime.requester(clientTls)) {
                    IncomingMeta seen = roundTrip(requester, urlOf(binding), replier);
                    assertNull(seen.peerFingerprint());
                    assertArrayEquals(generated.certificatePem(), Files.readAllBytes(certPem));
                }
            }
            runtime.shutdown(WAIT, S);
        }
    }
}
