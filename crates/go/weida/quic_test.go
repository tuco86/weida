//go:build integration

package weida

// The integration tier: Req/Rep over real QUIC on localhost, with a
// generated identity.

import (
	"errors"
	"slices"
	"testing"
)

func TestARoundTripOverQUICWithAPinnedKey(t *testing.T) {
	identity := must(GenerateIdentity())(t)
	fingerprint := must(identity.Fingerprint())(t)
	s := newServing(t, identity)
	trust := must(NewTrust())(t)
	if err := trust.AddPin(fingerprint); err != nil {
		t.Fatal(err)
	}
	tls := must(NewClientTLS(trust))(t)
	requester := must(s.runtime.Requester(tls))(t)
	if err := requester.Connect(t.Context(), "weida://"+fingerprint+"@"+s.binding.LocalAddr()+"/echo"); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	reverse := func(b []byte) []byte {
		r := slices.Clone(b)
		slices.Reverse(r)
		return r
	}
	served := async(func() ([]byte, error) {
		return serve(t.Context(), s.replier, reverse, &TransferMeta{ContentType: "text/plain"})
	})
	reply := must(requester.Request(t.Context(), []byte("ping")))(t)
	msg := must(reply.Collect(t.Context(), 1024))(t)
	if string(msg.Payload) != "gnip" || msg.Meta.ContentType == nil || *msg.Meta.ContentType != "text/plain" {
		t.Errorf("reply %q, meta %+v", msg.Payload, msg.Meta)
	}
	if body := must(await(served))(t); string(body) != "ping" {
		t.Errorf("served body %q", body)
	}
	requester.Close()
	s.close(t)
}

func TestAWrongPinIsUntrustedAndCarriesTheFingerprint(t *testing.T) {
	identity := must(GenerateIdentity())(t)
	fingerprint := must(identity.Fingerprint())(t)
	s := newServing(t, identity)
	other := must(GenerateIdentity())(t)
	trust := must(NewTrust())(t)
	if err := trust.AddPin(must(other.Fingerprint())(t)); err != nil {
		t.Fatal(err)
	}
	tls := must(NewClientTLS(trust))(t)
	requester := must(s.runtime.Requester(tls))(t)
	err := requester.Connect(t.Context(), "weida://"+s.binding.LocalAddr()+"/echo")
	if err == nil {
		_, err = requester.Request(t.Context(), []byte("hello"))
	}
	var failure *Error
	if !errors.As(err, &failure) || failure.Code != CodeUntrusted || failure.Fingerprint != fingerprint {
		t.Errorf("dialling a key not pinned: %v (%+v)", err, failure)
	}
	requester.Close()
	s.close(t)
}

// serving is a runtime serving QUIC on localhost with an identity, and a
// replier at /echo.
type serving struct {
	runtime  *Runtime
	listener *Listener
	binding  *Binding
	replier  *Replier
}

func newServing(t *testing.T, identity *Identity) *serving {
	t.Helper()
	tls := must(NewServerTLS(identity))(t)
	defer tls.Close()
	return newServingTLS(t, tls)
}

// newServingTLS serves on tls's terms, which the binding copies.
func newServingTLS(t *testing.T, tls *ServerTLS) *serving {
	t.Helper()
	s := &serving{}
	s.runtime = must(NewRuntime(nil))(t)
	s.listener = must(s.runtime.Listener())(t)
	s.binding = must(s.listener.BindQUIC(t.Context(), "127.0.0.1:0", tls))(t)
	s.replier = must(s.listener.Replier("/echo"))(t)
	return s
}

func (s *serving) close(t *testing.T) {
	t.Helper()
	s.replier.Close()
	s.binding.Close()
	s.listener.Close()
	if err := s.runtime.Shutdown(t.Context()); err != nil {
		t.Errorf("Shutdown: %v", err)
	}
}
