package weida

// #include "weida_go.h"
import "C"

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"time"
	"unsafe"
)

// RuntimeConfig is the runtime's configuration. NewRuntime(nil) takes
// weida's defaults; a config sets both fields as given.
type RuntimeConfig struct {
	// WorkerThreads is the number of the reactor's worker threads; 0 is
	// ErrRuntime.
	WorkerThreads uint32
	// ShutdownTimeout bounds Runtime.Shutdown's wait for idle connections,
	// rounded to milliseconds.
	ShutdownTimeout time.Duration
}

// Runtime is a weida runtime on a reactor of its own, and everything made
// from it keeps that reactor alive (R13).
type Runtime struct{ h handle }

// NewRuntime creates a runtime (weida_runtime_owned), with weida's defaults
// when cfg is nil.
func NewRuntime(cfg *RuntimeConfig) (*Runtime, error) {
	var e C.weida_go_error
	var c *C.weida_runtime_config
	if cfg != nil {
		if cfg.ShutdownTimeout < 0 {
			return nil, ErrInval
		}
		if err := check(C.go_runtime_config_new(&c, &e), &e); err != nil {
			return nil, err
		}
		defer C.weida_runtime_config_drop(c)
		if err := check(C.go_runtime_config_set_worker_threads(c, C.uint32_t(cfg.WorkerThreads), &e), &e); err != nil {
			return nil, err
		}
		ms := (cfg.ShutdownTimeout + time.Millisecond/2) / time.Millisecond
		if err := check(C.go_runtime_config_set_shutdown_timeout_ms(c, C.uint64_t(ms), &e), &e); err != nil {
			return nil, err
		}
	}
	var out *C.weida_runtime
	if err := check(C.go_runtime_owned(c, &out, &e), &e); err != nil {
		return nil, err
	}
	rt := &Runtime{}
	return track(rt, &rt.h, unsafe.Pointer(out), dropRuntime), nil
}

func dropRuntime(p unsafe.Pointer) { C.weida_runtime_drop((*C.weida_runtime)(p)) }

// Listener creates the runtime's listener.
func (rt *Runtime) Listener() (*Listener, error) {
	var out *C.weida_listener
	var e C.weida_go_error
	rc := rt.h.call(func(p unsafe.Pointer) C.int {
		return C.go_runtime_listener((*C.weida_runtime)(p), &out, &e)
	})
	if err := check(rc, &e); err != nil {
		return nil, err
	}
	l := &Listener{}
	return track(l, &l.h, unsafe.Pointer(out), dropListener), nil
}

// Requester creates a requester that dials on tls's terms.
func (rt *Runtime) Requester(tls *ClientTLS) (*Requester, error) {
	if tls == nil {
		return nil, ErrInval
	}
	var out *C.weida_requester
	var e C.weida_go_error
	rc := rt.h.call(func(rp unsafe.Pointer) C.int {
		return tls.h.call(func(tp unsafe.Pointer) C.int {
			return C.go_runtime_requester((*C.weida_runtime)(rp), (*C.weida_client_tls)(tp), &out, &e)
		})
	})
	if err := check(rc, &e); err != nil {
		return nil, err
	}
	r := &Requester{}
	return track(r, &r.h, unsafe.Pointer(out), dropRequester), nil
}

// Shutdown closes every endpoint and waits, bounded by the configured
// shutdown timeout and by ctx, for the connections to go idle. It consumes
// the runtime: the runtime is closed whether or not the wait finishes.
func (rt *Runtime) Shutdown(ctx context.Context) error {
	var op *C.weida_runtime_shutdown_op
	var e C.weida_go_error
	rc := rt.h.consume(func(p unsafe.Pointer) C.int {
		return C.go_runtime_shutdown((*C.weida_runtime)(p), &op, &e)
	})
	if err := check(rc, &e); err != nil {
		return err
	}
	defer C.weida_runtime_shutdown_op_drop(op)
	return wait(ctx, nil,
		func(out *C.int64_t, e *C.weida_go_error) C.int { return C.go_runtime_shutdown_op_fd(op, out, e) },
		func(e *C.weida_go_error) C.int { return C.go_runtime_shutdown_op_wait(op, e) })
}

// Close releases the caller's reference; the reactor lives on while
// anything made from the runtime does.
func (rt *Runtime) Close() error { return rt.h.close() }

// Listener is where a runtime serves: bindings and the endpoints
// registered on them.
type Listener struct{ h handle }

func dropListener(p unsafe.Pointer) { C.weida_listener_drop((*C.weida_listener)(p)) }

// BindQUIC serves QUIC on addr, e.g. "127.0.0.1:0", presenting tls's
// identity.
func (l *Listener) BindQUIC(ctx context.Context, addr string, tls *ServerTLS) (*Binding, error) {
	if tls == nil {
		return nil, ErrInval
	}
	caddr, err := cString(addr)
	if err != nil {
		return nil, err
	}
	defer freeString(caddr)
	var op *C.weida_listener_bind_quic_op
	var e C.weida_go_error
	rc := l.h.call(func(lp unsafe.Pointer) C.int {
		return tls.h.call(func(tp unsafe.Pointer) C.int {
			return C.go_listener_bind_quic((*C.weida_listener)(lp), caddr, (*C.weida_server_tls)(tp), &op, &e)
		})
	})
	if err := check(rc, &e); err != nil {
		return nil, err
	}
	defer C.weida_listener_bind_quic_op_drop(op)
	var out *C.weida_binding
	err = wait(ctx, &l.h,
		func(fd *C.int64_t, e *C.weida_go_error) C.int { return C.go_listener_bind_quic_op_fd(op, fd, e) },
		func(e *C.weida_go_error) C.int { return C.go_listener_bind_quic_op_wait(op, &out, e) })
	if err != nil {
		return nil, err
	}
	b := &Binding{}
	return track(b, &b.h, unsafe.Pointer(out), dropBinding), nil
}

// BindInproc answers on the in-process bus: a requester in this process
// reaches it at weida+inproc://<bus>/<path>.
func (l *Listener) BindInproc(bus string) (*LocalBinding, error) {
	cbus, err := cString(bus)
	if err != nil {
		return nil, err
	}
	defer freeString(cbus)
	var out *C.weida_local_binding
	var e C.weida_go_error
	rc := l.h.call(func(p unsafe.Pointer) C.int {
		return C.go_listener_bind_inproc((*C.weida_listener)(p), cbus, &out, &e)
	})
	if err := check(rc, &e); err != nil {
		return nil, err
	}
	b := &LocalBinding{}
	return track(b, &b.h, unsafe.Pointer(out), dropLocalBinding), nil
}

// Replier registers a replier at the endpoint path. Fails with
// ErrInvalidEndpointPath or ErrAlreadyRegistered.
func (l *Listener) Replier(path string) (*Replier, error) {
	cpath, err := cString(path)
	if err != nil {
		return nil, err
	}
	defer freeString(cpath)
	var out *C.weida_replier
	var e C.weida_go_error
	rc := l.h.call(func(p unsafe.Pointer) C.int {
		return C.go_listener_replier((*C.weida_listener)(p), cpath, &out, &e)
	})
	if err := check(rc, &e); err != nil {
		return nil, err
	}
	r := &Replier{}
	return track(r, &r.h, unsafe.Pointer(out), dropReplier), nil
}

// Close releases the listener; its bindings and endpoints stay.
func (l *Listener) Close() error { return l.h.close() }

// Binding is a QUIC binding: the socket a listener serves on.
type Binding struct{ h handle }

func dropBinding(p unsafe.Pointer) { C.weida_binding_drop((*C.weida_binding)(p)) }

// LocalAddr is the address the binding serves on, with the port resolved;
// "" once the binding is closed.
func (b *Binding) LocalAddr() string {
	var s string
	b.h.call(func(p unsafe.Pointer) C.int {
		var n C.size_t
		s = goString(C.weida_binding_local_addr((*C.weida_binding)(p), &n), n)
		return 0
	})
	return s
}

// Close stops serving on the binding.
func (b *Binding) Close() error { return b.h.close() }

// LocalBinding is an in-process binding: the bus a listener answers on.
type LocalBinding struct{ h handle }

func dropLocalBinding(p unsafe.Pointer) { C.weida_local_binding_drop((*C.weida_local_binding)(p)) }

// Bus is the bus the binding answers on; "" once the binding is closed.
func (b *LocalBinding) Bus() string {
	var s string
	b.h.call(func(p unsafe.Pointer) C.int {
		var n C.size_t
		s = goString(C.weida_local_binding_bus((*C.weida_local_binding)(p), &n), n)
		return 0
	})
	return s
}

// Close frees the bus name.
func (b *LocalBinding) Close() error { return b.h.close() }

// Trust is whom a dialler trusts: pins added to what each dialled address
// names.
type Trust struct{ h handle }

func dropTrust(p unsafe.Pointer) { C.weida_trust_drop((*C.weida_trust)(p)) }

// NewTrust creates a trust that accepts only what each dialled address
// names: a weida://sha256:...@host/path URL pins its key.
func NewTrust() (*Trust, error) {
	var out *C.weida_trust
	var e C.weida_go_error
	if err := check(C.go_trust_new(&out, &e), &e); err != nil {
		return nil, err
	}
	t := &Trust{}
	return track(t, &t.h, unsafe.Pointer(out), dropTrust), nil
}

// AddPin adds a pinned fingerprint: "sha256:" and 64 hex digits, otherwise
// ErrInvalidFingerprint.
func (t *Trust) AddPin(fingerprint string) error {
	cfp, err := cString(fingerprint)
	if err != nil {
		return err
	}
	defer freeString(cfp)
	var e C.weida_go_error
	rc := t.h.call(func(p unsafe.Pointer) C.int {
		return C.go_trust_add_pin((*C.weida_trust)(p), cfp, &e)
	})
	return check(rc, &e)
}

// AddAnchor adds the certificate authorities in pemBytes, PEM
// certificates: a peer whose chain leads to one of them and names the
// dialled host is accepted. The bytes are copied and parsed at each dial
// that uses the trust, which fails with ErrTLS when they hold no
// certificate or do not parse.
func (t *Trust) AddAnchor(pemBytes []byte) error {
	var e C.weida_go_error
	rc := t.h.call(func(p unsafe.Pointer) C.int {
		return C.go_trust_add_anchor((*C.weida_trust)(p), bytesIn(pemBytes), C.size_t(len(pemBytes)), &e)
	})
	return check(rc, &e)
}

// AddAnchorFile adds the certificate authorities in the PEM file at path.
// The path is kept and the file is read at each dial that uses the trust,
// which fails with ErrTLS when it is missing or does not parse.
func (t *Trust) AddAnchorFile(path string) error {
	cpath, err := cString(path)
	if err != nil {
		return err
	}
	defer freeString(cpath)
	var e C.weida_go_error
	rc := t.h.call(func(p unsafe.Pointer) C.int {
		return C.go_trust_add_anchor_file((*C.weida_trust)(p), cpath, &e)
	})
	return check(rc, &e)
}

// AddAnchorCertificates adds certs as certificate authorities: their PEM
// goes to AddAnchor. No certificate, or a nil one, is ErrInval. An
// *x509.CertPool is not taken: it cannot be enumerated.
func (t *Trust) AddAnchorCertificates(certs ...*x509.Certificate) error {
	if len(certs) == 0 {
		return ErrInval
	}
	var all []byte
	for _, c := range certs {
		if c == nil {
			return ErrInval
		}
		all = append(all, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: c.Raw})...)
	}
	return t.AddAnchor(all)
}

// Close releases the trust.
func (t *Trust) Close() error { return t.h.close() }

// Identity is a certificate chain and its private key.
type Identity struct{ h handle }

func dropIdentity(p unsafe.Pointer) { C.weida_identity_drop((*C.weida_identity)(p)) }

// GenerateIdentity generates a self-signed identity with a fresh key, made
// for pinning by fingerprint.
func GenerateIdentity() (*Identity, error) {
	var out *C.weida_identity
	var e C.weida_go_error
	if err := check(C.go_identity_generate(&out, &e), &e); err != nil {
		return nil, err
	}
	id := &Identity{}
	return track(id, &id.h, unsafe.Pointer(out), dropIdentity), nil
}

// IdentityFromPEM is an identity from PEM bytes: the certificate chain,
// leaf first, and the private key. The material is checked when it is
// first used.
func IdentityFromPEM(cert, key []byte) (*Identity, error) {
	var out *C.weida_identity
	var e C.weida_go_error
	rc := C.go_identity_from_pem(bytesIn(cert), C.size_t(len(cert)), bytesIn(key), C.size_t(len(key)), &out, &e)
	if err := check(rc, &e); err != nil {
		return nil, err
	}
	id := &Identity{}
	return track(id, &id.h, unsafe.Pointer(out), dropIdentity), nil
}

// GenerateIdentityFor generates a self-signed identity with a fresh key
// whose certificate also names names, DNS names or IP literals: a peer
// that takes the certificate as an anchor can then verify the host it
// dialled. No name is GenerateIdentity.
func GenerateIdentityFor(names ...string) (*Identity, error) {
	cnames := make([]*C.char, 0, len(names))
	defer func() {
		for _, s := range cnames {
			freeString(s)
		}
	}()
	for _, name := range names {
		s, err := cString(name)
		if err != nil {
			return nil, err
		}
		cnames = append(cnames, s)
	}
	var first **C.char
	if len(cnames) > 0 {
		first = (**C.char)(unsafe.Pointer(unsafe.SliceData(cnames)))
	}
	var out *C.weida_identity
	var e C.weida_go_error
	if err := check(C.go_identity_generate_for(first, C.size_t(len(cnames)), &out, &e), &e); err != nil {
		return nil, err
	}
	id := &Identity{}
	return track(id, &id.h, unsafe.Pointer(out), dropIdentity), nil
}

// IdentityFromPEMFile is an identity from one PEM file holding the
// certificate chain and the private key, the form ToPEM writes. Nothing is
// read here: a missing or bad file is ErrTLS at first use (Fingerprint,
// the PEM getters, a bind or a dial).
func IdentityFromPEMFile(path string) (*Identity, error) {
	cpath, err := cString(path)
	if err != nil {
		return nil, err
	}
	defer freeString(cpath)
	var out *C.weida_identity
	var e C.weida_go_error
	if err := check(C.go_identity_from_pem_file(cpath, &out, &e), &e); err != nil {
		return nil, err
	}
	id := &Identity{}
	return track(id, &id.h, unsafe.Pointer(out), dropIdentity), nil
}

// IdentityFromPEMFiles is an identity from a certificate chain file, leaf
// first, and a private key file. Nothing is read here: a missing or bad
// file is ErrTLS at first use (Fingerprint, the PEM getters, a bind or a
// dial).
func IdentityFromPEMFiles(certChain, key string) (*Identity, error) {
	ccert, err := cString(certChain)
	if err != nil {
		return nil, err
	}
	defer freeString(ccert)
	ckey, err := cString(key)
	if err != nil {
		return nil, err
	}
	defer freeString(ckey)
	var out *C.weida_identity
	var e C.weida_go_error
	if err := check(C.go_identity_from_pem_files(ccert, ckey, &out, &e), &e); err != nil {
		return nil, err
	}
	id := &Identity{}
	return track(id, &id.h, unsafe.Pointer(out), dropIdentity), nil
}

// IdentityFromTLS is an identity from a tls.Certificate: its chain, leaf
// first, and its private key, which must be an *ecdsa.PrivateKey, an
// *rsa.PrivateKey or an ed25519.PrivateKey. Any other key, such as a
// crypto.Signer that keeps the key in a device, is ErrInval: weida takes
// the key's bytes (docs/decisions/0047, case 2).
func IdentityFromTLS(cert tls.Certificate) (*Identity, error) {
	if len(cert.Certificate) == 0 {
		return nil, fmt.Errorf("weida: the tls.Certificate holds no certificate: %w", ErrInval)
	}
	var pkcs8 []byte
	var err error
	switch key := cert.PrivateKey.(type) {
	case *ecdsa.PrivateKey, *rsa.PrivateKey, ed25519.PrivateKey:
		pkcs8, err = x509.MarshalPKCS8PrivateKey(key)
		if err != nil {
			return nil, fmt.Errorf("weida: the private key does not encode: %w", err)
		}
	default:
		return nil, fmt.Errorf("weida: the private key is a %T, not an exportable *ecdsa.PrivateKey, *rsa.PrivateKey or ed25519.PrivateKey: a key that never leaves its store needs a signer handle, which weida does not have yet (docs/decisions/0047, case 2): %w", cert.PrivateKey, ErrInval)
	}
	defer clear(pkcs8)
	var chain []byte
	for _, der := range cert.Certificate {
		chain = append(chain, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})...)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: pkcs8})
	defer clear(keyPEM)
	return IdentityFromPEM(chain, keyPEM)
}

// Fingerprint is the identity's public-key fingerprint, "sha256:" text;
// ErrTLS when the certificate does not parse.
func (id *Identity) Fingerprint() (string, error) {
	var s string
	var e C.weida_go_error
	rc := id.h.call(func(p unsafe.Pointer) C.int {
		var out *C.char
		rc := C.go_identity_fingerprint((*C.weida_identity)(p), &out, &e)
		if rc == 0 {
			s = C.GoString(out)
		}
		return rc
	})
	return s, check(rc, &e)
}

// CertificatePEM is the identity's certificate chain as PEM, without the
// key: publishable, for a peer to anchor or pin. It is read anew on every
// call; ErrTLS when the source does not parse.
func (id *Identity) CertificatePEM() ([]byte, error) {
	return id.pemOut(func(p *C.weida_identity, out **C.weida_pem, e *C.weida_go_error) C.int {
		return C.go_identity_certificate_pem(p, out, e)
	})
}

// ToPEM is the chain and the private key as one PEM document, the form
// IdentityFromPEM and IdentityFromPEMFile read back. It holds the private
// key: wipe it with clear; every other copy is the caller's to wipe. It is
// read anew on every call; ErrTLS when the source does not parse.
func (id *Identity) ToPEM() ([]byte, error) {
	return id.pemOut(func(p *C.weida_identity, out **C.weida_pem, e *C.weida_go_error) C.int {
		return C.go_identity_to_pem(p, out, e)
	})
}

// pemOut copies the PEM that wrap writes into a Go slice and drops it,
// which wipes it, in the same cgo call.
func (id *Identity) pemOut(wrap func(*C.weida_identity, **C.weida_pem, *C.weida_go_error) C.int) ([]byte, error) {
	var data []byte
	var e C.weida_go_error
	rc := id.h.call(func(p unsafe.Pointer) C.int {
		var pm *C.weida_pem
		rc := wrap((*C.weida_identity)(p), &pm, &e)
		if rc == 0 {
			data = C.GoBytes(unsafe.Pointer(C.weida_pem_data(pm)), C.int(C.weida_pem_len(pm)))
			C.weida_pem_drop(pm)
		}
		return rc
	})
	if err := check(rc, &e); err != nil {
		return nil, err
	}
	return data, nil
}

// Certificates is the identity's certificate chain, leaf first, parsed
// from CertificatePEM. A certificate that does not parse is ErrTLS.
func (id *Identity) Certificates() ([]*x509.Certificate, error) {
	data, err := id.CertificatePEM()
	if err != nil {
		return nil, err
	}
	var certs []*x509.Certificate
	rest := data
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			return certs, nil
		}
		if block.Type != "CERTIFICATE" {
			continue
		}
		c, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, &Error{Code: CodeTLS, Message: fmt.Sprintf("certificate %d does not parse: %v", len(certs), err), Max: -1}
		}
		certs = append(certs, c)
	}
}

// TLSCertificate is the identity as a tls.Certificate, for Go's own crypto/tls.
// Its private key is a copy in Go memory: the caller wipes it if it must.
func (id *Identity) TLSCertificate() (tls.Certificate, error) {
	data, err := id.ToPEM()
	if err != nil {
		return tls.Certificate{}, err
	}
	defer clear(data)
	return tls.X509KeyPair(data, data)
}

// Close releases the identity.
func (id *Identity) Close() error { return id.h.close() }

// ClientTLS is the TLS terms a requester dials on.
type ClientTLS struct{ h handle }

func dropClientTLS(p unsafe.Pointer) { C.weida_client_tls_drop((*C.weida_client_tls)(p)) }

// NewClientTLS creates client terms that dial anonymously under trust,
// which is copied.
func NewClientTLS(trust *Trust) (*ClientTLS, error) {
	if trust == nil {
		return nil, ErrInval
	}
	var out *C.weida_client_tls
	var e C.weida_go_error
	rc := trust.h.call(func(p unsafe.Pointer) C.int {
		return C.go_client_tls_new((*C.weida_trust)(p), &out, &e)
	})
	if err := check(rc, &e); err != nil {
		return nil, err
	}
	t := &ClientTLS{}
	return track(t, &t.h, unsafe.Pointer(out), dropClientTLS), nil
}

// SetIdentity makes the terms present identity to the servers they dial,
// which is copied: it affects requesters made afterwards. A server that
// requires a client key refuses the dial with ErrTLS otherwise. A nil
// identity is ErrInval.
func (t *ClientTLS) SetIdentity(identity *Identity) error {
	if identity == nil {
		return ErrInval
	}
	var e C.weida_go_error
	rc := t.h.call(func(p unsafe.Pointer) C.int {
		return identity.h.call(func(q unsafe.Pointer) C.int {
			return C.go_client_tls_set_identity((*C.weida_client_tls)(p), (*C.weida_identity)(q), &e)
		})
	})
	return check(rc, &e)
}

// Close releases the terms.
func (t *ClientTLS) Close() error { return t.h.close() }

// ServerTLS is the TLS terms a QUIC binding serves on.
type ServerTLS struct{ h handle }

func dropServerTLS(p unsafe.Pointer) { C.weida_server_tls_drop((*C.weida_server_tls)(p)) }

// NewServerTLS creates server terms that present identity, which is
// copied, and accept anonymous clients.
func NewServerTLS(identity *Identity) (*ServerTLS, error) {
	if identity == nil {
		return nil, ErrInval
	}
	var out *C.weida_server_tls
	var e C.weida_go_error
	rc := identity.h.call(func(p unsafe.Pointer) C.int {
		return C.go_server_tls_new((*C.weida_identity)(p), &out, &e)
	})
	if err := check(rc, &e); err != nil {
		return nil, err
	}
	t := &ServerTLS{}
	return track(t, &t.h, unsafe.Pointer(out), dropServerTLS), nil
}

// RequireClient requires every client to present a key trust accepts,
// pinned or chaining to an anchor. The trust is copied and the last call
// wins. A refused client gets ErrTLS on its side and nothing reaches a
// replier; an empty trust fails the bind with ErrTLS. A nil trust is
// ErrInval.
func (t *ServerTLS) RequireClient(trust *Trust) error {
	if trust == nil {
		return ErrInval
	}
	var e C.weida_go_error
	rc := t.h.call(func(p unsafe.Pointer) C.int {
		return trust.h.call(func(q unsafe.Pointer) C.int {
			return C.go_server_tls_require_client((*C.weida_server_tls)(p), (*C.weida_trust)(q), &e)
		})
	})
	return check(rc, &e)
}

// RequireAnyKey requires every client to present a key and prove it holds
// it, whatever the key; the key is then PeerFingerprint and PeerChain of
// the request's metadata. The last call wins. A client without a key gets
// ErrTLS on its side and nothing reaches a replier.
func (t *ServerTLS) RequireAnyKey() error {
	var e C.weida_go_error
	rc := t.h.call(func(p unsafe.Pointer) C.int {
		return C.go_server_tls_require_any_key((*C.weida_server_tls)(p), &e)
	})
	return check(rc, &e)
}

// Close releases the terms.
func (t *ServerTLS) Close() error { return t.h.close() }

// goString copies a string a handle lends (R7); "" for NULL.
func goString(s *C.char, n C.size_t) string {
	if s == nil {
		return ""
	}
	return C.GoStringN(s, C.int(n))
}
