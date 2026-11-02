//go:build integration

package weida

// The integration tier: client keys, anchors and the presented chain over
// real QUIC on localhost (docs/decisions/0047).

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestAPinnedClientKeyIsAcceptedAndTheServerSeesItsKeyAndChain(t *testing.T) {
	server := must(GenerateIdentity())(t)
	client := must(GenerateIdentity())(t)
	clientFingerprint := must(client.Fingerprint())(t)
	clientCerts := must(client.Certificates())(t)

	s := newServingTLS(t, requiringPinned(t, server, client))
	requester := newRequester(t, s, pinning(t, server), client)
	meta := echoOnce(t, s, requester, urlOf(s))
	if meta.PeerFingerprint == nil || *meta.PeerFingerprint != clientFingerprint {
		t.Errorf("the server saw the key %v, want %s", meta.PeerFingerprint, clientFingerprint)
	}
	chain := must(meta.PeerChain())(t)
	if len(chain) != 1 || !chain[0].Equal(clientCerts[0]) {
		t.Errorf("the server saw the chain %v, want the client's certificate", chain)
	}
	requester.Close()
	s.close(t)
}

func TestAnAnonymousOrUnknownClientIsRefusedWithTLSAndNothingReachesTheServer(t *testing.T) {
	server := must(GenerateIdentity())(t)
	client := must(GenerateIdentity())(t)
	stranger := must(GenerateIdentity())(t)

	s := newServingTLS(t, requiringPinned(t, server, client))
	for _, c := range []struct {
		name     string
		identity *Identity
	}{
		{"an anonymous client", nil},
		{"an unknown client", stranger},
	} {
		requester := newRequester(t, s, pinning(t, server), c.identity)
		if err := refused(t, requester, urlOf(s)); !errors.Is(err, ErrTLS) {
			t.Errorf("%s: %v, want ErrTLS", c.name, err)
		}
		requester.Close()
	}
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	if request, err := s.replier.Accept(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Accept after the refusals: %v, want the deadline", err)
		if err == nil {
			request.Close()
		}
	}
	s.close(t)
}

func TestABindingThatRequiresClientsFromAnEmptyTrustDoesNotBind(t *testing.T) {
	server := must(GenerateIdentity())(t)
	terms := must(NewServerTLS(server))(t)
	empty := must(NewTrust())(t)
	if err := terms.RequireClient(empty); err != nil {
		t.Fatal(err)
	}
	rt := must(NewRuntime(nil))(t)
	listener := must(rt.Listener())(t)
	_, err := listener.BindQUIC(t.Context(), "127.0.0.1:0", terms)
	var failure *Error
	if !errors.As(err, &failure) || !errors.Is(err, ErrTLS) {
		t.Fatalf("BindQUIC with an empty client trust: %v, want ErrTLS", err)
	}
	if !strings.Contains(failure.Message, "client trust is empty") {
		t.Errorf("the message does not say the trust is empty: %q", failure.Message)
	}
	listener.Close()
	if err := rt.Shutdown(t.Context()); err != nil {
		t.Errorf("Shutdown: %v", err)
	}
}

func TestAnyKeyExposesTheFingerprintAndTheChain(t *testing.T) {
	server := must(GenerateIdentity())(t)
	client := must(GenerateIdentity())(t)
	clientFingerprint := must(client.Fingerprint())(t)

	terms := must(NewServerTLS(server))(t)
	if err := terms.RequireAnyKey(); err != nil {
		t.Fatal(err)
	}
	s := newServingTLS(t, terms)
	anonymous := newRequester(t, s, pinning(t, server), nil)
	if err := refused(t, anonymous, urlOf(s)); !errors.Is(err, ErrTLS) {
		t.Errorf("an anonymous client: %v, want ErrTLS", err)
	}
	anonymous.Close()

	requester := newRequester(t, s, pinning(t, server), client)
	meta := echoOnce(t, s, requester, urlOf(s))
	if meta.PeerFingerprint == nil || *meta.PeerFingerprint != clientFingerprint {
		t.Errorf("the server saw the key %v, want %s", meta.PeerFingerprint, clientFingerprint)
	}
	if chain := must(meta.PeerChain())(t); len(chain) != 1 {
		t.Errorf("the server saw %d certificates, want 1", len(chain))
	}
	requester.Close()
	s.close(t)
}

func TestAnAnchorAcceptsTheDialledNameAndRefusesAnother(t *testing.T) {
	named := must(GenerateIdentityFor("127.0.0.1"))(t)
	namedCerts := must(named.Certificates())(t)
	s := newServing(t, named)
	anchored := must(NewTrust())(t)
	if err := anchored.AddAnchorCertificates(namedCerts[0]); err != nil {
		t.Fatal(err)
	}
	requester := newRequester(t, s, anchored, nil)
	meta := echoOnce(t, s, requester, urlOf(s))
	if meta.PeerChainDER != nil {
		t.Errorf("an anonymous client left the chain %d certificates long", len(meta.PeerChainDER))
	}
	requester.Close()
	s.close(t)

	other := must(GenerateIdentityFor("localhost"))(t)
	otherFingerprint := must(other.Fingerprint())(t)
	otherCerts := must(other.Certificates())(t)
	s = newServing(t, other)
	anchoredOther := must(NewTrust())(t)
	if err := anchoredOther.AddAnchorCertificates(otherCerts[0]); err != nil {
		t.Fatal(err)
	}
	requester = newRequester(t, s, anchoredOther, nil)
	err := refused(t, requester, urlOf(s))
	var failure *Error
	if !errors.As(err, &failure) || failure.Code != CodeUntrusted || failure.Fingerprint != otherFingerprint {
		t.Errorf("dialling a name the certificate does not carry: %v (%+v)", err, failure)
	}
	requester.Close()
	s.close(t)
}

func TestIdentityAndAnchorFilesReadBack(t *testing.T) {
	generated := must(GenerateIdentityFor("127.0.0.1"))(t)
	want := must(generated.Fingerprint())(t)
	dir := t.TempDir()
	idPath := filepath.Join(dir, "id.pem")
	certPath := filepath.Join(dir, "cert.pem")
	idPEM := must(generated.ToPEM())(t)
	err := os.WriteFile(idPath, idPEM, 0o600)
	clear(idPEM)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(certPath, must(generated.CertificatePEM())(t), 0o600); err != nil {
		t.Fatal(err)
	}

	fromOne := must(IdentityFromPEMFile(idPath))(t)
	if got := must(fromOne.Fingerprint())(t); got != want {
		t.Errorf("IdentityFromPEMFile: %s, want %s", got, want)
	}
	fromTwo := must(IdentityFromPEMFiles(certPath, idPath))(t)
	if got := must(fromTwo.Fingerprint())(t); got != want {
		t.Errorf("IdentityFromPEMFiles: %s, want %s", got, want)
	}

	s := newServing(t, fromOne)
	anchored := must(NewTrust())(t)
	if err := anchored.AddAnchorFile(certPath); err != nil {
		t.Fatal(err)
	}
	requester := newRequester(t, s, anchored, nil)
	echoOnce(t, s, requester, urlOf(s))
	requester.Close()
	s.close(t)

	missing := must(IdentityFromPEMFile(filepath.Join(dir, "missing.pem")))(t)
	if _, err := missing.Fingerprint(); !errors.Is(err, ErrTLS) {
		t.Errorf("Fingerprint of a missing file: %v, want ErrTLS", err)
	}
}

// requiringPinned is server's terms, requiring a client key pinned to
// client's. The binding copies them, so the caller keeps nothing to close.
func requiringPinned(t *testing.T, server, client *Identity) *ServerTLS {
	t.Helper()
	trust := must(NewTrust())(t)
	defer trust.Close()
	if err := trust.AddPin(must(client.Fingerprint())(t)); err != nil {
		t.Fatal(err)
	}
	terms := must(NewServerTLS(server))(t)
	if err := terms.RequireClient(trust); err != nil {
		t.Fatal(err)
	}
	return terms
}

// pinning is the trust of a client that dials server by its pinned key.
func pinning(t *testing.T, server *Identity) *Trust {
	t.Helper()
	trust := must(NewTrust())(t)
	if err := trust.AddPin(must(server.Fingerprint())(t)); err != nil {
		t.Fatal(err)
	}
	return trust
}

// newRequester is a requester of s's runtime that dials on trust and
// presents identity, unless it is nil.
func newRequester(t *testing.T, s *serving, trust *Trust, identity *Identity) *Requester {
	t.Helper()
	terms := must(NewClientTLS(trust))(t)
	if identity != nil {
		if err := terms.SetIdentity(identity); err != nil {
			t.Fatal(err)
		}
	}
	return must(s.runtime.Requester(terms))(t)
}

func urlOf(s *serving) string {
	return "weida://" + s.binding.LocalAddr() + "/echo"
}

// echoOnce sends one request to s and returns the metadata the server saw.
func echoOnce(t *testing.T, s *serving, requester *Requester, url string) IncomingMeta {
	t.Helper()
	if err := requester.Connect(t.Context(), url); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	served := async(func() (IncomingMeta, error) {
		return serveSeeing(t.Context(), s.replier)
	})
	reply := must(requester.Request(t.Context(), []byte("ping")))(t)
	msg := must(reply.Collect(t.Context(), 1024))(t)
	if string(msg.Payload) != "hi" {
		t.Errorf("reply %q, want hi", msg.Payload)
	}
	return must(await(served))(t)
}

// refused dials url and sends a request; the error is whichever step
// failed first, nil when none did.
func refused(t *testing.T, requester *Requester, url string) error {
	t.Helper()
	if err := requester.Connect(t.Context(), url); err != nil {
		return err
	}
	reply, err := requester.Request(t.Context(), []byte("ping"))
	if err != nil {
		return err
	}
	reply.Close()
	return nil
}

// serveSeeing answers one request with "hi" as serve does, and returns the
// metadata the request carried, read right after the accept.
func serveSeeing(ctx context.Context, r *Replier) (IncomingMeta, error) {
	request, err := r.Accept(ctx)
	if err != nil {
		return IncomingMeta{}, err
	}
	defer request.Close()
	meta, err := request.Meta()
	if err != nil {
		return IncomingMeta{}, err
	}
	body, err := request.TakeBody()
	if err != nil {
		return IncomingMeta{}, err
	}
	if _, err := readAll(ctx, body, 3); err != nil {
		return IncomingMeta{}, err
	}
	reply, err := request.Reply(ctx, nil)
	if err != nil {
		return IncomingMeta{}, err
	}
	defer reply.Close()
	if _, err := reply.WriteContext(ctx, []byte("hi")); err != nil {
		return IncomingMeta{}, err
	}
	delivery, err := reply.Finish()
	if err != nil {
		return IncomingMeta{}, err
	}
	return meta, delivery.Delivered(ctx)
}
