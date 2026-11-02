package net.doodleshnookie.weida;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;

/**
 * PEM out of DER and certificates out of PEM or DER, with only what Android
 * API 24 has: no {@code java.util.Base64}, no {@code java.nio.file}.
 */
final class Pem {
    private static final byte[] ALPHABET =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/".getBytes(StandardCharsets.US_ASCII);
    private static final int LINE = 64;
    private static final String END_CERTIFICATE = "-----END CERTIFICATE-----";

    private Pem() {}

    /**
     * One block per DER: {@code -----BEGIN label-----}, the standard base64
     * alphabet with padding in lines of 64 characters, {@code -----END label-----},
     * each line ending in a line feed. US-ASCII bytes.
     */
    static byte[] encode(String label, byte[]... ders) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (byte[] der : ders) {
            ascii(out, "-----BEGIN " + label + "-----\n");
            int column = 0;
            for (int i = 0; i < der.length; i += 3) {
                int b0 = der[i] & 0xFF;
                int b1 = i + 1 < der.length ? der[i + 1] & 0xFF : 0;
                int b2 = i + 2 < der.length ? der[i + 2] & 0xFF : 0;
                int group = (b0 << 16) | (b1 << 8) | b2;
                int[] quad = {
                    ALPHABET[(group >> 18) & 0x3F],
                    ALPHABET[(group >> 12) & 0x3F],
                    i + 1 < der.length ? ALPHABET[(group >> 6) & 0x3F] : '=',
                    i + 2 < der.length ? ALPHABET[group & 0x3F] : '=',
                };
                for (int c : quad) {
                    out.write(c);
                    if (++column == LINE) {
                        out.write('\n');
                        column = 0;
                    }
                }
            }
            if (column > 0) {
                out.write('\n');
            }
            ascii(out, "-----END " + label + "-----\n");
        }
        return out.toByteArray();
    }

    private static void ascii(ByteArrayOutputStream out, String text) {
        byte[] bytes = text.getBytes(StandardCharsets.US_ASCII);
        out.write(bytes, 0, bytes.length);
    }

    /**
     * Every {@code CERTIFICATE} block of a PEM document, in order, each parsed
     * alone so that a failure names the block's position; throws {@code TLS}
     * when one does not parse.
     */
    static X509Certificate[] certificates(byte[] pem) {
        String text = new String(pem, StandardCharsets.US_ASCII);
        List<X509Certificate> out = new ArrayList<X509Certificate>();
        int from = 0;
        for (int end; (end = text.indexOf(END_CERTIFICATE, from)) >= 0; ) {
            end += END_CERTIFICATE.length();
            byte[] block = text.substring(from, end).getBytes(StandardCharsets.US_ASCII);
            out.add(certificate(block, out.size()));
            from = end;
        }
        return out.toArray(new X509Certificate[0]);
    }

    /**
     * One certificate of a PEM block or a DER, {@code index} being its place
     * in a chain; throws {@code TLS} when it does not parse.
     */
    static X509Certificate certificate(byte[] der, int index) {
        try {
            return (X509Certificate) CertificateFactory.getInstance("X.509")
                    .generateCertificate(new ByteArrayInputStream(der));
        } catch (CertificateException e) {
            throw tlsFailure("certificate " + index + " does not parse: " + e.getMessage());
        }
    }

    /** A {@code TLS} failure no C call reported, so without a record. */
    static WeidaException tlsFailure(String message) {
        return new WeidaException(WeidaException.Code.TLS.errno(), message, 0, null, -1, 0);
    }
}
