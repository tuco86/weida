package net.doodleshnookie.weida;

/**
 * Every native method of the JNI glue ({@code src/main/c/weida_jni.c}), one per
 * function of {@code weida.h} or a few of them. Handles are the C pointers as
 * {@code long}. No method blocks except {@link #poll}: every try and op wait
 * passes {@code timeout_ms = 0}. {@code -WEIDA_EAGAIN} is returned, not thrown:
 * a method returning a handle returns 0, one returning a count -1, one
 * returning nothing 0 (again) or 1 (done). Any other failure throws a
 * {@link WeidaException}.
 */
final class Native {
    private Native() {}

    static {
        String path = System.getProperty("weida.jni.path");
        if (path != null) {
            System.load(path);
        } else {
            // On Android, the APK's jniLibs; libweida.so sits beside it.
            System.loadLibrary("weida_jni");
        }
    }

    // The handle kinds of drop and fd: the numbers of weida_jni.c's enum kind.
    static final int RUNTIME_CONFIG = 1;
    static final int RUNTIME = 2;
    static final int SHUTDOWN_OP = 3;
    static final int TRUST = 4;
    static final int IDENTITY = 5;
    static final int CLIENT_TLS = 6;
    static final int SERVER_TLS = 7;
    static final int TRANSFER_META = 8;
    static final int LISTENER = 9;
    static final int BIND_QUIC_OP = 10;
    static final int BINDING = 11;
    static final int LOCAL_BINDING = 12;
    static final int REPLIER = 13;
    static final int INCOMING_REQUEST = 14;
    static final int REPLY_OP = 15;
    static final int REQUESTER = 16;
    static final int CONNECT_OP = 17;
    static final int REQUEST_OP = 18;
    static final int INCOMING_META = 19;
    static final int INCOMING_TRANSFER = 20;
    static final int COLLECT_OP = 21;
    static final int OUTGOING_TRANSFER = 22;
    static final int DELIVERY = 23;
    static final int DELIVERED_OP = 24;

    static native String version();

    static native String strerror(int code);

    static native void drop(int kind, long handle);

    static native long fd(int kind, long handle);

    static native long runtimeConfigNew();

    static native void runtimeConfigSetWorkerThreads(long cfg, int threads);

    static native void runtimeConfigSetShutdownTimeoutMs(long cfg, long ms);

    static native long runtimeOwned(long cfg);

    static native long runtimeListener(long rt);

    static native long runtimeRequester(long rt, long tls);

    static native long runtimeShutdown(long rt);

    static native int runtimeShutdownOpWait(long op);

    static native long trustNew();

    static native void trustAddPin(long trust, String fingerprint);

    static native void trustAddAnchor(long trust, byte[] pem);

    static native void trustAddAnchorFile(long trust, String path);

    static native long identityGenerate();

    static native long identityGenerateFor(String[] names);

    static native long identityFromPem(byte[] cert, byte[] key);

    static native long identityFromPemFile(String path);

    static native long identityFromPemFiles(String certChain, String key);

    static native String identityFingerprint(long identity);

    static native byte[] identityCertificatePem(long identity);

    static native byte[] identityToPem(long identity);

    static native long clientTlsNew(long trust);

    static native void clientTlsSetIdentity(long tls, long identity);

    static native long serverTlsNew(long identity);

    static native void serverTlsRequireClient(long tls, long trust);

    static native void serverTlsRequireAnyKey(long tls);

    static native long transferMetaNew();

    static native void transferMetaSetContentType(long meta, String contentType);

    static native long listenerBindQuic(long listener, String addr, long tls);

    static native long listenerBindQuicOpWait(long op);

    static native long listenerBindInproc(long listener, String bus);

    static native long listenerReplier(long listener, String path);

    static native String bindingLocalAddr(long binding);

    static native String localBindingBus(long binding);

    static native String replierPath(long replier);

    static native long replierAccept(long replier);

    static native long incomingRequestMeta(long request);

    static native long incomingRequestTakeBody(long request);

    static native long incomingRequestReply(long request, long meta);

    static native long incomingRequestReplyOpWait(long op);

    static native long requesterConnect(long requester, String url);

    static native int requesterConnectOpWait(long op);

    static native long requesterRequest(long requester, byte[] body);

    static native long requesterRequestOpWait(long op);

    /** {@code which}: 0 endpoint, 1 content type, 2 topic, 3 peer fingerprint. */
    static native String incomingMetaString(long meta, int which);

    /** Writes content_len and sequence to {@code out}; bit 0 and bit 1 say which are present. */
    static native int incomingMetaNumbers(long meta, long[] out);

    /** The DER of each certificate the sender presented, leaf first; {@code null} when the chain is absent. */
    static native byte[][] incomingMetaPeerChain(long meta);

    static native long incomingTransferMeta(long transfer);

    static native int incomingTransferRead(long transfer, byte[] buf, int off, int len);

    static native long incomingTransferCollect(long transfer, long maxBytes);

    static native byte[] incomingTransferCollectOpWait(long op, long[] metaOut);

    static native int outgoingTransferWrite(long transfer, byte[] buf, int off, int len);

    static native long outgoingTransferFinish(long transfer);

    static native long deliveryDelivered(long delivery);

    static native int deliveryDeliveredOpWait(long op);

    static native long[] wakeCreate();

    static native void wake(long fd);

    static native void drain(long fd);

    /** weida_poll: the number of ready descriptors, each marked in {@code ready}; -1 on EINTR. */
    static native int poll(long[] fds, int n, long timeoutMs, boolean[] ready);
}
