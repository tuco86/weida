package net.doodleshnookie.weida;

/** An in-process binding: the bus a listener answers on. Closing it frees the bus name. */
public final class LocalBinding extends Handle {
    LocalBinding(long ptr) {
        super(Native.LOCAL_BINDING, ptr);
    }

    /** The bus the binding answers on. */
    public String bus() {
        long b = acquire();
        try {
            return Native.localBindingBus(b);
        } finally {
            release();
        }
    }
}
