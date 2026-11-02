package net.doodleshnookie.weida;

import java.io.File;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Objects;

/** A certificate chain and its private key. */
public final class Identity extends Handle {
    private Identity(long ptr) {
        super(Native.IDENTITY, ptr);
    }

    /** A self-signed identity with a fresh key, held in memory: made for pinning by fingerprint. */
    public static Identity generate() {
        return new Identity(Native.identityGenerate());
    }

    /**
     * An identity from PEM bytes: the certificate chain, leaf first, and the
     * private key. Both are copied; the material is checked when first used.
     */
    public static Identity fromPem(byte[] cert, byte[] key) {
        Objects.requireNonNull(cert, "cert");
        Objects.requireNonNull(key, "key");
        return new Identity(Native.identityFromPem(cert, key));
    }

    /**
     * A self-signed identity with a fresh key whose certificate names the
     * given hosts (DNS names or IP addresses), so that a peer that takes the
     * certificate as a trust anchor can verify the host it dialled. No names
     * is {@link #generate()}. A name holding a NUL character throws {@code INVAL}.
     */
    public static Identity generateFor(String... names) {
        Objects.requireNonNull(names, "names");
        for (String name : names) {
            Objects.requireNonNull(name, "name");
        }
        return new Identity(Native.identityGenerateFor(names));
    }

    /**
     * An identity from one PEM file holding the chain, leaf first, and the
     * private key, the form {@link #toPem()} writes. Nothing is read here; a
     * missing or bad file throws {@code TLS} at first use ({@link #fingerprint()},
     * the PEM getters, a bind or a dial).
     */
    public static Identity fromPemFile(File path) {
        Objects.requireNonNull(path, "path");
        return new Identity(Native.identityFromPemFile(path.getPath()));
    }

    /**
     * An identity from a PEM file of the certificate chain, leaf first, and a
     * PEM file of the private key. Nothing is read here; a missing or bad
     * file throws {@code TLS} at first use ({@link #fingerprint()}, the PEM
     * getters, a bind or a dial).
     */
    public static Identity fromPemFiles(File certChain, File key) {
        Objects.requireNonNull(certChain, "certChain");
        Objects.requireNonNull(key, "key");
        return new Identity(Native.identityFromPemFiles(certChain.getPath(), key.getPath()));
    }

    /**
     * An identity from Java's key types: the chain, leaf first, and its
     * private key. The key must be exportable ({@code getEncoded()} PKCS#8);
     * the copies made here are wiped before this returns.
     *
     * @throws IllegalArgumentException for no certificate or a key that cannot be exported
     */
    public static Identity from(PrivateKey key, X509Certificate... chain) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(chain, "chain");
        for (X509Certificate cert : chain) {
            Objects.requireNonNull(cert, "chain element");
        }
        if (chain.length == 0) {
            throw new IllegalArgumentException("Identity.from needs at least one certificate, leaf first");
        }
        byte[] pkcs8 = key.getEncoded();
        if (pkcs8 == null || !"PKCS#8".equals(key.getFormat())) {
            if (pkcs8 != null) {
                Arrays.fill(pkcs8, (byte) 0);
            }
            throw new IllegalArgumentException("this private key cannot be exported (no PKCS#8 encoding): "
                    + "a key that never leaves its store, such as an Android Keystore key, needs a signer handle, "
                    + "which weida does not have yet (docs/decisions/0047, case 2)");
        }
        byte[] keyPem = null;
        try {
            byte[][] ders = new byte[chain.length][];
            for (int i = 0; i < chain.length; i++) {
                try {
                    ders[i] = chain[i].getEncoded();
                } catch (CertificateEncodingException e) {
                    throw new IllegalArgumentException(e);
                }
            }
            byte[] certPem = Pem.encode("CERTIFICATE", ders);
            keyPem = Pem.encode("PRIVATE KEY", pkcs8);
            return fromPem(certPem, keyPem);
        } finally {
            Arrays.fill(pkcs8, (byte) 0);
            if (keyPem != null) {
                Arrays.fill(keyPem, (byte) 0);
            }
        }
    }

    /**
     * The private-key entry {@code alias} of a key store, with its X.509
     * chain, leaf first. A wrong password is the store's
     * {@code UnrecoverableKeyException}.
     *
     * @throws IllegalArgumentException when the entry is no private key with an X.509 chain
     */
    public static Identity fromKeyStore(KeyStore store, String alias, char[] password)
            throws GeneralSecurityException {
        Objects.requireNonNull(store, "store");
        Objects.requireNonNull(alias, "alias");
        Key key = store.getKey(alias, password);
        Certificate[] chain = store.getCertificateChain(alias);
        if (!(key instanceof PrivateKey) || chain == null || chain.length == 0) {
            throw new IllegalArgumentException("no private key entry with an X.509 chain named " + alias);
        }
        X509Certificate[] x509 = new X509Certificate[chain.length];
        for (int i = 0; i < chain.length; i++) {
            if (!(chain[i] instanceof X509Certificate)) {
                throw new IllegalArgumentException("no private key entry with an X.509 chain named " + alias);
            }
            x509[i] = (X509Certificate) chain[i];
        }
        return from((PrivateKey) key, x509);
    }

    /** The public-key fingerprint, {@code sha256:} text. Throws {@code TLS} when the certificate does not parse. */
    public String fingerprint() {
        long id = acquire();
        try {
            return Native.identityFingerprint(id);
        } finally {
            release();
        }
    }

    /** The certificate chain, leaf first, as PEM; read anew on every call. Throws {@code TLS} when it does not read. */
    public byte[] certificatePem() {
        long id = acquire();
        try {
            return Native.identityCertificatePem(id);
        } finally {
            release();
        }
    }

    /**
     * The certificate chain and the private key as one PEM document, the form
     * {@link #fromPemFile(File)} reads. Holds the private key: wipe it with
     * {@code Arrays.fill(pem, (byte) 0)}; every other copy is the caller's to wipe.
     * Read anew on every call; throws {@code TLS} when it does not read.
     */
    public byte[] toPem() {
        long id = acquire();
        try {
            return Native.identityToPem(id);
        } finally {
            release();
        }
    }

    /** The certificate chain, leaf first, as Java certificates. Throws {@code TLS} when one does not parse. */
    public X509Certificate[] certificates() {
        return Pem.certificates(certificatePem());
    }
}
