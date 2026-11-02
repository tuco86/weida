package weida

// The unit tier: key material in and out of an identity, in Go's own types
// (docs/decisions/0047). No file or socket is touched.

import (
	"bytes"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"math/big"
	"net"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestToPEMReadsBackWithTheSameFingerprint(t *testing.T) {
	id := must(GenerateIdentity())(t)
	defer id.Close()
	want := must(id.Fingerprint())(t)
	data := must(id.ToPEM())(t)
	defer clear(data)
	if !bytes.Contains(data, []byte("PRIVATE KEY")) {
		t.Errorf("ToPEM holds no private key:\n%s", data)
	}
	back := must(IdentityFromPEM(data, data))(t)
	defer back.Close()
	if got := must(back.Fingerprint())(t); got != want {
		t.Errorf("fingerprint after a round trip %s, want %s", got, want)
	}
	certPEM := must(id.CertificatePEM())(t)
	if !bytes.Contains(certPEM, []byte("-----BEGIN CERTIFICATE-----")) || bytes.Contains(certPEM, []byte("PRIVATE KEY")) {
		t.Errorf("CertificatePEM is not the chain alone:\n%s", certPEM)
	}
}

func TestCertificatesAreTheIdentitysKey(t *testing.T) {
	id := must(GenerateIdentity())(t)
	defer id.Close()
	certs := must(id.Certificates())(t)
	if len(certs) != 1 {
		t.Fatalf("%d certificates, want 1", len(certs))
	}
	if got, want := spkiFingerprint(certs[0]), must(id.Fingerprint())(t); got != want {
		t.Errorf("the certificate's key is %s, the identity's %s", got, want)
	}
}

func TestGenerateIdentityForNamesTheCertificate(t *testing.T) {
	id := must(GenerateIdentityFor("localhost", "127.0.0.1"))(t)
	defer id.Close()
	certs := must(id.Certificates())(t)
	if len(certs) != 1 {
		t.Fatalf("%d certificates, want 1", len(certs))
	}
	if !slices.Equal(certs[0].DNSNames, []string{"localhost"}) {
		t.Errorf("DNS names %q, want [localhost]", certs[0].DNSNames)
	}
	if len(certs[0].IPAddresses) == 0 || !certs[0].IPAddresses[0].Equal(net.ParseIP("127.0.0.1")) {
		t.Errorf("IP addresses %v, want [127.0.0.1]", certs[0].IPAddresses)
	}
}

func TestIdentityFromTLSTakesGosKeyTypes(t *testing.T) {
	ecKey := must(ecdsa.GenerateKey(elliptic.P256(), rand.Reader))(t)
	edPub, edKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name string
		pub  crypto.PublicKey
		key  crypto.PrivateKey
	}{
		{"ecdsa", &ecKey.PublicKey, ecKey},
		{"ed25519", edPub, edKey},
	} {
		t.Run(c.name, func(t *testing.T) {
			der := selfSigned(t, c.pub, c.key)
			parsed := must(x509.ParseCertificate(der))(t)
			want := spkiFingerprint(parsed)
			id := must(IdentityFromTLS(tls.Certificate{Certificate: [][]byte{der}, PrivateKey: c.key}))(t)
			defer id.Close()
			if got := must(id.Fingerprint())(t); got != want {
				t.Errorf("fingerprint %s, want %s", got, want)
			}
			back := must(IdentityFromTLS(must(id.TLSCertificate())(t)))(t)
			defer back.Close()
			if got := must(back.Fingerprint())(t); got != want {
				t.Errorf("fingerprint after TLSCertificate %s, want %s", got, want)
			}
		})
	}
}

// signer hides its key's type: crypto/tls signs with it, weida cannot read
// the key out of it.
type signer struct{ crypto.Signer }

func TestASignerThatIsNotExportableNamesCase2(t *testing.T) {
	key := must(ecdsa.GenerateKey(elliptic.P256(), rand.Reader))(t)
	_, err := IdentityFromTLS(tls.Certificate{Certificate: [][]byte{{1}}, PrivateKey: signer{key}})
	if !errors.Is(err, ErrInval) {
		t.Fatalf("IdentityFromTLS with a signer: %v, want ErrInval", err)
	}
	if !strings.Contains(err.Error(), "case 2") {
		t.Errorf("the error does not name case 2: %v", err)
	}
}

func TestArgumentErrors(t *testing.T) {
	id := must(GenerateIdentity())(t)
	defer id.Close()
	trust := must(NewTrust())(t)
	defer trust.Close()
	clientTerms := must(NewClientTLS(trust))(t)
	defer clientTerms.Close()
	serverTerms := must(NewServerTLS(id))(t)
	defer serverTerms.Close()

	_, errNul := GenerateIdentityFor("a\x00b")
	_, errNoCert := IdentityFromTLS(tls.Certificate{})
	for name, err := range map[string]error{
		"a NUL in a name":               errNul,
		"a tls.Certificate without one": errNoCert,
		"no anchor certificate":         trust.AddAnchorCertificates(),
		"a nil anchor certificate":      trust.AddAnchorCertificates(nil),
		"a nil identity":                clientTerms.SetIdentity(nil),
		"a nil client trust":            serverTerms.RequireClient(nil),
	} {
		if !errors.Is(err, ErrInval) {
			t.Errorf("%s: %v, want ErrInval", name, err)
		}
	}
	if err := serverTerms.RequireClient(trust); err != nil {
		t.Errorf("RequireClient with an empty trust: %v", err)
	}
	if err := serverTerms.RequireAnyKey(); err != nil {
		t.Errorf("RequireAnyKey: %v", err)
	}
}

// spkiFingerprint is the fingerprint weida gives a key: "sha256:" and the
// hash of the certificate's SubjectPublicKeyInfo.
func spkiFingerprint(c *x509.Certificate) string {
	sum := sha256.Sum256(c.RawSubjectPublicKeyInfo)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// selfSigned is the DER of a certificate for pub that priv signed.
func selfSigned(t *testing.T, pub crypto.PublicKey, priv crypto.PrivateKey) []byte {
	t.Helper()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		DNSNames:     []string{"go"},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, pub, priv)
	if err != nil {
		t.Fatal(err)
	}
	return der
}
