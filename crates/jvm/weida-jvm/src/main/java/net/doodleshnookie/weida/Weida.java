package net.doodleshnookie.weida;

/**
 * weida for the JVM and Android: a Java API over libweida, the C ABI of weida,
 * through one JNI library (docs/libraries/weida-jvm.md).
 *
 * <p>Every class mirrors one C handle and is {@link AutoCloseable}; a closed or
 * consumed object throws {@link WeidaException} with
 * {@link WeidaException.Code#CLOSED} without calling C. Every waiting operation
 * exists twice: {@code xAsync(..)} returns a {@link java.util.concurrent.CompletableFuture},
 * whose {@code cancel} cancels the operation, and {@code x(.., timeout, unit)}
 * blocks for it, throwing {@code AGAIN} when the timeout passes.
 *
 * <p>The JNI library is {@code System.load}ed from the system property
 * {@code weida.jni.path} when set, else {@code System.loadLibrary("weida_jni")};
 * libweida sits beside it.
 */
public final class Weida {
    private Weida() {}

    /** The version of the loaded libweida, e.g. {@code "0.1.0-alpha.11-dev"}. */
    public static String version() {
        return Native.version();
    }

    /** The description of a status code, either sign; {@code "unknown error"} for one libweida does not define. */
    public static String strerror(int code) {
        return Native.strerror(code);
    }
}
