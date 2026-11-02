package net.doodleshnookie.weida;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Req/Rep over the in-process transport: every wait through the waiter thread, no sockets. */
@Timeout(1)
class InprocTest {
    static final long WAIT = 5;
    static final TimeUnit S = TimeUnit.SECONDS;

    /** A runtime with a replier at {@code /echo} and a requester connected to it over a bus of its own. */
    static final class Pair implements AutoCloseable {
        final Runtime runtime = Runtime.owned();
        final Listener listener = runtime.listener();
        final String bus = "test-" + UUID.randomUUID();
        final LocalBinding binding = listener.bindInproc(bus);
        final Replier replier = listener.replier("/echo");
        final Requester requester;

        Pair() throws InterruptedException {
            try (Trust trust = new Trust(); ClientTls tls = new ClientTls(trust)) {
                requester = runtime.requester(tls);
            }
            requester.connect("weida+inproc://" + bus + "/echo", WAIT, S);
        }

        @Override
        public void close() {
            requester.close();
            replier.close();
            binding.close();
            listener.close();
            try {
                runtime.shutdown(WAIT, S);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        }
    }

    /** {@code body} on a thread of its own, its outcome as a future. */
    static <T> CompletableFuture<T> inThread(Callable<T> body) {
        CompletableFuture<T> future = new CompletableFuture<>();
        Thread thread = new Thread(() -> {
            try {
                future.complete(body.call());
            } catch (Throwable e) {
                future.completeExceptionally(e);
            }
        });
        thread.setDaemon(true);
        thread.start();
        return future;
    }

    /** Reads {@code body} to its end, 3 bytes at a time. */
    static byte[] readAll(IncomingTransfer body) throws InterruptedException {
        ByteArrayOutputStream received = new ByteArrayOutputStream();
        byte[] piece = new byte[3];
        for (int n; (n = body.read(piece, 0, piece.length, WAIT, S)) != -1; ) {
            received.write(piece, 0, n);
        }
        return received.toByteArray();
    }

    /** Answers one request with {@code answer(body)}, blocking; returns the body. */
    static String serve(Replier replier, UnaryOperator<byte[]> answer, String contentType)
            throws InterruptedException {
        IncomingRequest request = replier.accept(WAIT, S);
        byte[] body = readAll(request.takeBody());
        OutgoingTransfer reply = request.reply(new TransferMeta().contentType(contentType), WAIT, S);
        reply.writeAll(answer.apply(body), WAIT, S);
        reply.finish().delivered(WAIT, S);
        return new String(body, UTF_8);
    }

    /** Answers one request with its body reversed, without blocking; completes with the body. */
    static CompletableFuture<String> serveAsync(Replier replier) {
        return replier.acceptAsync().thenCompose(request -> request.takeBody().collectAsync(1024)
                .thenCompose(body -> request.replyAsync(null)
                        .thenCompose(reply -> reply.writeAllAsync(reversed(body.payload()))
                                .thenCompose(done -> reply.finish().deliveredAsync()))
                        .thenApply(done -> new String(body.payload(), UTF_8))));
    }

    static byte[] reversed(byte[] bytes) {
        byte[] out = new byte[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            out[i] = bytes[bytes.length - 1 - i];
        }
        return out;
    }

    /** A request whose answer the test does not look at. */
    static CompletableFuture<Void> requestQuietly(Requester requester, String body) {
        return requester.requestAsync(body.getBytes(UTF_8))
                .thenCompose(reply -> reply.collectAsync(1024))
                .handle((message, error) -> {
                    if (error != null && !(unwrap(error) instanceof WeidaException)) {
                        throw new AssertionError(error);
                    }
                    return null;
                });
    }

    static Throwable unwrap(Throwable error) {
        while ((error instanceof ExecutionException || error instanceof java.util.concurrent.CompletionException)
                && error.getCause() != null) {
            error = error.getCause();
        }
        return error;
    }

    static WeidaException.Code codeOf(Throwable error) {
        Throwable cause = unwrap(error);
        assertInstanceOf(WeidaException.class, cause);
        return ((WeidaException) cause).code();
    }

    @Test
    void aStreamedRoundTripCarriesTheBodyAndTheContentType() throws Exception {
        try (Pair p = new Pair()) {
            CompletableFuture<String> served = inThread(
                    () -> serve(p.replier, body -> new String(body, UTF_8).toUpperCase().getBytes(UTF_8), "text/plain"));
            IncomingTransfer reply = p.requester.request("ping".getBytes(UTF_8), WAIT, S);
            assertEquals("text/plain", reply.meta().contentType());
            Message message = reply.collect(1024, WAIT, S);
            assertEquals("PING", new String(message.payload(), UTF_8));
            assertEquals("text/plain", message.meta().contentType());
            assertEquals("ping", served.get(WAIT, S));
        }
    }

    @Test
    void readsOf3BytesReassembleTheBody() throws Exception {
        try (Pair p = new Pair()) {
            CompletableFuture<Void> client = requestQuietly(p.requester, "0123456789");
            IncomingRequest request = p.replier.accept(WAIT, S);
            IncomingTransfer body = request.takeBody();
            StringBuilder pieces = new StringBuilder();
            byte[] piece = new byte[3];
            for (int n; (n = body.read(piece, 0, piece.length, WAIT, S)) != -1; ) {
                assertTrue(n > 0 && n <= 3, "read " + n);
                pieces.append(new String(piece, 0, n, UTF_8));
            }
            assertEquals("0123456789", pieces.toString());
            assertEquals(-1, body.read(piece, 0, piece.length, WAIT, S));
            request.close();
            client.get(WAIT, S);
        }
    }

    @Test
    void aCollectOverItsCeilingIsLimitExceeded() throws Exception {
        try (Pair p = new Pair()) {
            CompletableFuture<Void> client = requestQuietly(p.requester, "four");
            IncomingRequest request = p.replier.accept(WAIT, S);
            WeidaException e = assertThrows(WeidaException.class, () -> request.takeBody().collect(2, WAIT, S));
            assertEquals(WeidaException.Code.LIMIT_EXCEEDED, e.code());
            request.close();
            client.get(WAIT, S);
        }
    }

    @Test
    void aReplyClosedUnfinishedReadsAsCanceled() throws Exception {
        try (Pair p = new Pair()) {
            CompletableFuture<Void> server = inThread(() -> {
                IncomingRequest request = p.replier.accept(WAIT, S);
                OutgoingTransfer reply = request.reply(null, WAIT, S);
                reply.writeAll("ha".getBytes(UTF_8), WAIT, S);
                reply.close();
                return null;
            });
            IncomingTransfer reply = p.requester.request("go".getBytes(UTF_8), WAIT, S);
            byte[] buf = new byte[64];
            WeidaException e = assertThrows(WeidaException.class, () -> {
                while (reply.read(buf, 0, buf.length, WAIT, S) != -1) {
                    // drain until the reset arrives
                }
            });
            assertEquals(WeidaException.Code.CANCELED, e.code());
            server.get(WAIT, S);
        }
    }

    @Test
    void aSecondReplyIsClosed() throws Exception {
        try (Pair p = new Pair()) {
            CompletableFuture<Void> client = requestQuietly(p.requester, "once");
            IncomingRequest request = p.replier.accept(WAIT, S);
            request.reply(null, WAIT, S).close();
            WeidaException e = assertThrows(WeidaException.class, () -> request.reply(null, WAIT, S));
            assertEquals(WeidaException.Code.CLOSED, e.code());
            client.get(WAIT, S);
        }
    }

    @Test
    void twoConcurrentAcceptsOnOneReplierServeTwoRequests() throws Exception {
        try (Pair p = new Pair()) {
            List<CompletableFuture<String>> accepts = List.of(serveAsync(p.replier), serveAsync(p.replier));
            List<CompletableFuture<IncomingTransfer>> replies = List.of(
                    p.requester.requestAsync("one".getBytes(UTF_8)), p.requester.requestAsync("two".getBytes(UTF_8)));
            String[] payloads = new String[2];
            for (int i = 0; i < 2; i++) {
                payloads[i] = new String(replies.get(i).get(WAIT, S).collect(1024, WAIT, S).payload(), UTF_8);
            }
            Arrays.sort(payloads);
            assertEquals(List.of("eno", "owt"), Arrays.asList(payloads));
            String[] bodies = {accepts.get(0).get(WAIT, S), accepts.get(1).get(WAIT, S)};
            Arrays.sort(bodies);
            assertEquals(List.of("one", "two"), Arrays.asList(bodies));
        }
    }

    @Test
    void aCancelledRequestEndsAndTheRuntimeStillShutsDown() throws Exception {
        try (Pair p = new Pair()) {
            CompletableFuture<IncomingTransfer> pending = p.requester.requestAsync("unanswered".getBytes(UTF_8));
            assertTrue(pending.cancel(false));
            assertThrows(CancellationException.class, pending::join);
        }
    }

    @Test
    void closingAReplierCompletesItsPendingAcceptWithClosed() throws Exception {
        try (Pair p = new Pair()) {
            CompletableFuture<IncomingRequest> accept = p.replier.acceptAsync();
            p.replier.close();
            ExecutionException e = assertThrows(ExecutionException.class, () -> accept.get(WAIT, S));
            assertEquals(WeidaException.Code.CLOSED, codeOf(e));
        }
    }

    @Test
    void anAcceptThatTimesOutThrowsAgainAndTheNextOneServes() throws Exception {
        try (Pair p = new Pair()) {
            WeidaException e = assertThrows(WeidaException.class, () -> p.replier.accept(10, TimeUnit.MILLISECONDS));
            assertEquals(WeidaException.Code.AGAIN, e.code());
            CompletableFuture<Void> client = requestQuietly(p.requester, "after");
            p.replier.accept(WAIT, S).close();
            client.get(WAIT, S);
        }
    }

    @Test
    void aBindingNeverClosedIsDroppedOnceUnreachableAndFreesItsBus() throws Exception {
        try (Pair p = new Pair()) {
            String bus = "test-" + UUID.randomUUID();
            p.listener.bindInproc(bus); // never closed, unreachable at once
            while (true) {
                System.gc();
                try {
                    p.listener.bindInproc(bus).close();
                    return;
                } catch (WeidaException e) {
                    Thread.sleep(10); // the bus is still taken: the cleaner has not run yet
                }
            }
        }
    }
}
