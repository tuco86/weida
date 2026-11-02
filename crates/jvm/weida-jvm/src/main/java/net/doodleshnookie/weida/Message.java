package net.doodleshnookie.weida;

/** A collected payload and the metadata of the transfer it consumed, copied out of libweida at once. */
public final class Message {
    private final byte[] payload;
    private final IncomingMeta meta;

    Message(byte[] payload, IncomingMeta meta) {
        this.payload = payload;
        this.meta = meta;
    }

    /** The payload; the array is the message's own, not copied again. */
    public byte[] payload() {
        return payload;
    }

    public IncomingMeta meta() {
        return meta;
    }
}
