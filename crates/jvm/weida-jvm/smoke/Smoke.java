import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import net.doodleshnookie.weida.Binding;
import net.doodleshnookie.weida.ClientTls;
import net.doodleshnookie.weida.Identity;
import net.doodleshnookie.weida.Listener;
import net.doodleshnookie.weida.Message;
import net.doodleshnookie.weida.OutgoingTransfer;
import net.doodleshnookie.weida.Replier;
import net.doodleshnookie.weida.Requester;
import net.doodleshnookie.weida.Runtime;
import net.doodleshnookie.weida.ServerTls;
import net.doodleshnookie.weida.TransferMeta;
import net.doodleshnookie.weida.Trust;
import net.doodleshnookie.weida.Weida;

/**
 * What the jar must do on its own: load the JNI library and its libweida, and
 * make one QUIC round trip with a generated, pinned identity. Checks that the
 * jar's Implementation-Version is the version of the libweida it loaded.
 *
 * <p>Usage: java -Dweida.jni.path=<libweida_jni> -cp weida-jvm.jar smoke/Smoke.java
 */
public class Smoke {
    public static void main(String[] args) throws Exception {
        String version = Weida.version();
        String jarVersion = Weida.class.getPackage().getImplementationVersion();
        if (!version.equals(jarVersion)) {
            throw new IllegalStateException("the jar is " + jarVersion + ", libweida " + version);
        }
        long wait = 10;
        TimeUnit unit = TimeUnit.SECONDS;
        try (Runtime runtime = Runtime.owned();
                Listener listener = runtime.listener();
                Identity identity = Identity.generate();
                ServerTls serverTls = new ServerTls(identity);
                Binding binding = listener.bindQuic("127.0.0.1:0", serverTls, wait, unit);
                Replier replier = listener.replier("/echo");
                Trust trust = new Trust().addPin(identity.fingerprint());
                ClientTls clientTls = new ClientTls(trust);
                Requester requester = runtime.requester(clientTls)) {
            requester.connect("weida://" + identity.fingerprint() + "@" + binding.localAddr() + "/echo", wait, unit);

            // replyAsync consumes the request.
            CompletableFuture<Void> served = replier.acceptAsync().thenCompose(request -> request.takeBody()
                    .collectAsync(1024)
                    .thenCompose(body -> request.replyAsync(new TransferMeta().contentType("text/plain"))
                            .thenCompose(reply -> reverse(reply, body))));

            Message message = requester.request("ping".getBytes(StandardCharsets.UTF_8), wait, unit)
                    .collect(1024, wait, unit);
            served.get(wait, unit);
            String text = new String(message.payload(), StandardCharsets.UTF_8);
            if (!text.equals("gnip") || !"text/plain".equals(message.meta().contentType())) {
                throw new IllegalStateException("unexpected reply " + text + " (" + message.meta().contentType() + ")");
            }
            runtime.shutdown(wait, unit);
        }
        System.out.println("jvm smoke: ok libweida " + version);
    }

    private static CompletableFuture<Void> reverse(OutgoingTransfer reply, Message body) {
        byte[] payload = body.payload();
        byte[] reversed = new byte[payload.length];
        for (int i = 0; i < payload.length; i++) {
            reversed[i] = payload[payload.length - 1 - i];
        }
        return reply.writeAllAsync(reversed).thenCompose(done -> reply.finish().deliveredAsync());
    }
}
