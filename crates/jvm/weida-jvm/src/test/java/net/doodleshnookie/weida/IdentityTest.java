package net.doodleshnookie.weida;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Arrays;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Key material in and out of an identity, in Java's key types: no files, no sockets. */
@Timeout(1)
class IdentityTest {
    /** A key that never leaves its store: no encoding. */
    static final class NonExportableKey implements PrivateKey {
        private static final long serialVersionUID = 1L;

        @Override
        public String getAlgorithm() {
            return "EC";
        }

        @Override
        public String getFormat() {
            return null;
        }

        @Override
        public byte[] getEncoded() {
            return null;
        }
    }

    /** The private key of an identity, decoded from the {@code PRIVATE KEY} block of its PEM. */
    private static PrivateKey privateKeyOf(Identity id) throws Exception {
        String pem = new String(id.toPem(), US_ASCII);
        String begin = "-----BEGIN PRIVATE KEY-----";
        int from = pem.indexOf(begin) + begin.length();
        String body = pem.substring(from, pem.indexOf("-----END PRIVATE KEY-----"));
        byte[] der = Base64.getMimeDecoder().decode(body);
        return KeyFactory.getInstance(id.certificates()[0].getPublicKey().getAlgorithm())
                .generatePrivate(new PKCS8EncodedKeySpec(der));
    }

    private static String fingerprintOf(X509Certificate cert) throws Exception {
        return "sha256:"
                + HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(cert.getPublicKey().getEncoded()));
    }

    @Test
    void toPemReadsBackWithTheSameFingerprint() {
        try (Identity id = Identity.generate()) {
            byte[] pem = id.toPem();
            try (Identity back = Identity.fromPem(pem, pem)) {
                assertEquals(id.fingerprint(), back.fingerprint());
            } finally {
                Arrays.fill(pem, (byte) 0);
            }
            assertTrue(new String(id.toPem(), US_ASCII).contains("PRIVATE KEY"));
            String certificates = new String(id.certificatePem(), US_ASCII);
            assertTrue(certificates.contains("-----BEGIN CERTIFICATE-----"));
            assertFalse(certificates.contains("PRIVATE KEY"));
        }
    }

    @Test
    void certificatesAreTheIdentitysKey() throws Exception {
        try (Identity id = Identity.generate()) {
            X509Certificate[] certificates = id.certificates();
            assertEquals(1, certificates.length);
            assertEquals(id.fingerprint(), fingerprintOf(certificates[0]));
        }
    }

    @Test
    void generateForNamesTheCertificate() throws Exception {
        try (Identity id = Identity.generateFor("localhost", "127.0.0.1")) {
            var names = id.certificates()[0].getSubjectAlternativeNames();
            assertTrue(names.contains(List.of(2, "localhost")), names.toString());
            assertTrue(names.contains(List.of(7, "127.0.0.1")), names.toString());
        }
    }

    @Test
    void fromTakesJavasKeyTypes() throws Exception {
        try (Identity id = Identity.generate();
                Identity back = Identity.from(privateKeyOf(id), id.certificates())) {
            assertEquals(id.fingerprint(), back.fingerprint());
        }
    }

    @Test
    void fromKeyStoreReadsAPkcs12Entry() throws Exception {
        try (Identity id = Identity.generate()) {
            KeyStore store = KeyStore.getInstance("PKCS12");
            store.load(null, null);
            store.setKeyEntry("me", privateKeyOf(id), "pw".toCharArray(), id.certificates());
            try (Identity back = Identity.fromKeyStore(store, "me", "pw".toCharArray())) {
                assertEquals(id.fingerprint(), back.fingerprint());
            }
            assertThrows(IllegalArgumentException.class,
                    () -> Identity.fromKeyStore(store, "nobody", "pw".toCharArray()));
        }
    }

    @Test
    void aKeyThatCannotBeExportedNamesCase2() {
        try (Identity id = Identity.generate()) {
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> Identity.from(new NonExportableKey(), id.certificates()));
            assertTrue(e.getMessage().contains("case 2"), e.getMessage());
        }
    }

    @Test
    void argumentErrors() throws Exception {
        assertThrows(NullPointerException.class, () -> Identity.generateFor((String) null));
        WeidaException e = assertThrows(WeidaException.class, () -> Identity.generateFor("a\0b"));
        assertEquals(WeidaException.Code.INVAL, e.code());
        try (Identity id = Identity.generate()) {
            PrivateKey key = privateKeyOf(id);
            assertThrows(IllegalArgumentException.class, () -> Identity.from(key));
            try (ServerTls tls = new ServerTls(id);
                    Trust trust = new Trust()) {
                tls.requireClient(trust);
                tls.requireAnyKey();
            }
        }
    }
}
