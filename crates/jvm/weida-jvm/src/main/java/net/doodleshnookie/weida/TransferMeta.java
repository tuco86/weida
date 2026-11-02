package net.doodleshnookie.weida;

/** The metadata an outgoing transfer carries in its header; turned into a C handle per call. */
public final class TransferMeta {
    private String contentType;

    public TransferMeta() {}

    /** Sets the opaque content type label; returns this value. */
    public TransferMeta contentType(String contentType) {
        this.contentType = contentType;
        return this;
    }

    /** The content type label, or {@code null}. */
    public String contentType() {
        return contentType;
    }

    /** A C transfer meta holding these fields; the caller drops it. 0 for {@code null}, which is empty. */
    static long handle(TransferMeta meta) {
        if (meta == null) {
            return 0;
        }
        long handle = Native.transferMetaNew();
        try {
            if (meta.contentType != null) {
                Native.transferMetaSetContentType(handle, meta.contentType);
            }
        } catch (RuntimeException e) {
            Native.drop(Native.TRANSFER_META, handle);
            throw e;
        }
        return handle;
    }
}
