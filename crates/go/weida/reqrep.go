package weida

// #include "weida_go.h"
import "C"

import (
	"context"
	"crypto/x509"
	"fmt"
	"io"
	"unsafe"
)

// Requester is the requesting half of Req/Rep: it dials repliers and sends
// them requests.
type Requester struct{ h handle }

func dropRequester(p unsafe.Pointer) { C.weida_requester_drop((*C.weida_requester)(p)) }

// Connect dials url, e.g. weida://sha256:...@127.0.0.1:7443/echo or
// weida+inproc://bus/echo. A peer whose key is not trusted is
// ErrUntrusted. The runtime redials it when the connection is lost.
func (r *Requester) Connect(ctx context.Context, url string) error {
	curl, err := cString(url)
	if err != nil {
		return err
	}
	defer freeString(curl)
	var op *C.weida_requester_connect_op
	var e C.weida_go_error
	rc := r.h.call(func(p unsafe.Pointer) C.int {
		return C.go_requester_connect((*C.weida_requester)(p), curl, &op, &e)
	})
	if err := check(rc, &e); err != nil {
		return err
	}
	defer C.weida_requester_connect_op_drop(op)
	return wait(ctx, &r.h,
		func(fd *C.int64_t, e *C.weida_go_error) C.int { return C.go_requester_connect_op_fd(op, fd, e) },
		func(e *C.weida_go_error) C.int { return C.go_requester_connect_op_wait(op, e) })
}

// Request sends body, which is copied, as one request and returns the reply
// as soon as its header arrives. A request the replier dropped is
// ErrNoReply.
func (r *Requester) Request(ctx context.Context, body []byte) (*IncomingTransfer, error) {
	var op *C.weida_requester_request_op
	var e C.weida_go_error
	rc := r.h.call(func(p unsafe.Pointer) C.int {
		return C.go_requester_request((*C.weida_requester)(p), bytesIn(body), C.size_t(len(body)), &op, &e)
	})
	if err := check(rc, &e); err != nil {
		return nil, err
	}
	defer C.weida_requester_request_op_drop(op)
	var out *C.weida_incoming_transfer
	err := wait(ctx, &r.h,
		func(fd *C.int64_t, e *C.weida_go_error) C.int { return C.go_requester_request_op_fd(op, fd, e) },
		func(e *C.weida_go_error) C.int { return C.go_requester_request_op_wait(op, &out, e) })
	if err != nil {
		return nil, err
	}
	t := &IncomingTransfer{}
	return track(t, &t.h, unsafe.Pointer(out), dropIncomingTransfer), nil
}

// Close drops the requester's connections.
func (r *Requester) Close() error { return r.h.close() }

// Replier is the replying half of Req/Rep, serving one endpoint path.
type Replier struct{ h handle }

func dropReplier(p unsafe.Pointer) { C.weida_replier_drop((*C.weida_replier)(p)) }

// Path is the endpoint path the replier serves; "" once it is closed.
func (r *Replier) Path() string {
	var s string
	r.h.call(func(p unsafe.Pointer) C.int {
		var n C.size_t
		s = goString(C.weida_replier_path((*C.weida_replier)(p), &n), n)
		return 0
	})
	return s
}

// Accept takes the next request as soon as its header arrives. Several
// goroutines may accept on one replier; a canceled Accept loses nothing.
func (r *Replier) Accept(ctx context.Context) (*IncomingRequest, error) {
	var out *C.weida_incoming_request
	err := wait(ctx, &r.h,
		func(fd *C.int64_t, e *C.weida_go_error) C.int {
			return C.go_replier_fd((*C.weida_replier)(r.h.ptr), fd, e)
		},
		func(e *C.weida_go_error) C.int {
			return r.h.call(func(p unsafe.Pointer) C.int {
				return C.go_replier_accept((*C.weida_replier)(p), &out, e)
			})
		})
	if err != nil {
		return nil, err
	}
	q := &IncomingRequest{}
	return track(q, &q.h, unsafe.Pointer(out), dropIncomingRequest), nil
}

// Close releases the replier; a pending Accept returns ErrClosed.
func (r *Replier) Close() error { return r.h.close() }

// IncomingRequest is one accepted request: its body and the right to reply.
// Closed without a reply, it answers the requester with ErrNoReply.
type IncomingRequest struct{ h handle }

func dropIncomingRequest(p unsafe.Pointer) {
	C.weida_incoming_request_drop((*C.weida_incoming_request)(p))
}

// Meta is the request's metadata.
func (q *IncomingRequest) Meta() (IncomingMeta, error) {
	var m *C.weida_incoming_meta
	var e C.weida_go_error
	rc := q.h.call(func(p unsafe.Pointer) C.int {
		return C.go_incoming_request_meta((*C.weida_incoming_request)(p), &m, &e)
	})
	if err := check(rc, &e); err != nil {
		return IncomingMeta{}, err
	}
	defer C.weida_incoming_meta_drop(m)
	return copyMeta(m), nil
}

// TakeBody detaches the request's body, to be read or collected; a second
// call is ErrClosed.
func (q *IncomingRequest) TakeBody() (*IncomingTransfer, error) {
	var out *C.weida_incoming_transfer
	var e C.weida_go_error
	rc := q.h.call(func(p unsafe.Pointer) C.int {
		return C.go_incoming_request_take_body((*C.weida_incoming_request)(p), &out, &e)
	})
	if err := check(rc, &e); err != nil {
		return nil, err
	}
	t := &IncomingTransfer{}
	return track(t, &t.h, unsafe.Pointer(out), dropIncomingTransfer), nil
}

// Reply opens the reply stream with meta, or empty metadata when meta is
// nil. It consumes the request: a second Reply is ErrClosed. Closing the
// returned transfer unfinished resets the reply with ErrCanceled.
func (q *IncomingRequest) Reply(ctx context.Context, meta *TransferMeta) (*OutgoingTransfer, error) {
	cmeta, err := meta.open()
	if err != nil {
		return nil, err
	}
	defer C.weida_transfer_meta_drop(cmeta)
	var op *C.weida_incoming_request_reply_op
	var e C.weida_go_error
	rc := q.h.consume(func(p unsafe.Pointer) C.int {
		return C.go_incoming_request_reply((*C.weida_incoming_request)(p), cmeta, &op, &e)
	})
	if err := check(rc, &e); err != nil {
		return nil, err
	}
	defer C.weida_incoming_request_reply_op_drop(op)
	var out *C.weida_outgoing_transfer
	err = wait(ctx, nil,
		func(fd *C.int64_t, e *C.weida_go_error) C.int { return C.go_incoming_request_reply_op_fd(op, fd, e) },
		func(e *C.weida_go_error) C.int { return C.go_incoming_request_reply_op_wait(op, &out, e) })
	if err != nil {
		return nil, err
	}
	t := &OutgoingTransfer{}
	return track(t, &t.h, unsafe.Pointer(out), dropOutgoingTransfer), nil
}

// Close releases the request; one not replied to answers ErrNoReply.
func (q *IncomingRequest) Close() error { return q.h.close() }

// IncomingTransfer is an inbound payload, read like a socket or collected
// whole. Closed unread, it refuses the payload with ErrRejected.
type IncomingTransfer struct{ h handle }

func dropIncomingTransfer(p unsafe.Pointer) {
	C.weida_incoming_transfer_drop((*C.weida_incoming_transfer)(p))
}

// Meta is the transfer's metadata.
func (t *IncomingTransfer) Meta() (IncomingMeta, error) {
	var m *C.weida_incoming_meta
	var e C.weida_go_error
	rc := t.h.call(func(p unsafe.Pointer) C.int {
		return C.go_incoming_transfer_meta((*C.weida_incoming_transfer)(p), &m, &e)
	})
	if err := check(rc, &e); err != nil {
		return IncomingMeta{}, err
	}
	defer C.weida_incoming_meta_drop(m)
	return copyMeta(m), nil
}

// Read implements io.Reader, waiting without bound for some of the payload:
// io.EOF at its end, ErrCanceled when the peer canceled it.
func (t *IncomingTransfer) Read(p []byte) (int, error) {
	return t.ReadContext(context.Background(), p)
}

// ReadContext is Read, waiting until ctx ends.
func (t *IncomingTransfer) ReadContext(ctx context.Context, p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	var n C.size_t
	err := wait(ctx, &t.h,
		func(fd *C.int64_t, e *C.weida_go_error) C.int {
			return C.go_incoming_transfer_fd((*C.weida_incoming_transfer)(t.h.ptr), fd, e)
		},
		func(e *C.weida_go_error) C.int {
			return t.h.call(func(ptr unsafe.Pointer) C.int {
				return C.go_incoming_transfer_read((*C.weida_incoming_transfer)(ptr), (*C.uint8_t)(unsafe.Pointer(unsafe.SliceData(p))), C.size_t(len(p)), &n, e)
			})
		})
	if err != nil {
		return 0, err
	}
	if n == 0 {
		return 0, io.EOF
	}
	return int(n), nil
}

// Collect reads the whole remaining payload, refusing more than maxBytes
// with ErrLimitExceeded. It consumes the transfer.
func (t *IncomingTransfer) Collect(ctx context.Context, maxBytes uint64) (*Message, error) {
	var op *C.weida_incoming_transfer_collect_op
	var e C.weida_go_error
	rc := t.h.consume(func(p unsafe.Pointer) C.int {
		return C.go_incoming_transfer_collect((*C.weida_incoming_transfer)(p), C.uint64_t(maxBytes), &op, &e)
	})
	if err := check(rc, &e); err != nil {
		return nil, err
	}
	defer C.weida_incoming_transfer_collect_op_drop(op)
	var msg *C.weida_msg
	err := wait(ctx, nil,
		func(fd *C.int64_t, e *C.weida_go_error) C.int { return C.go_incoming_transfer_collect_op_fd(op, fd, e) },
		func(e *C.weida_go_error) C.int { return C.go_incoming_transfer_collect_op_wait(op, &msg, e) })
	if err != nil {
		return nil, err
	}
	defer C.weida_msg_drop(msg)
	payload := make([]byte, int(C.weida_msg_len(msg)))
	if len(payload) > 0 {
		copy(payload, unsafe.Slice((*byte)(unsafe.Pointer(C.weida_msg_data(msg))), len(payload)))
	}
	var m *C.weida_incoming_meta
	if err := check(C.go_msg_meta(msg, &m, &e), &e); err != nil {
		return nil, err
	}
	defer C.weida_incoming_meta_drop(m)
	return &Message{Payload: payload, Meta: copyMeta(m)}, nil
}

// Close releases the transfer; an unread payload is refused with
// ErrRejected.
func (t *IncomingTransfer) Close() error { return t.h.close() }

// OutgoingTransfer is an outbound payload, written like a socket and
// finished with Finish. Closed unfinished, it resets the payload with
// ErrCanceled.
type OutgoingTransfer struct{ h handle }

func dropOutgoingTransfer(p unsafe.Pointer) {
	C.weida_outgoing_transfer_drop((*C.weida_outgoing_transfer)(p))
}

// Write implements io.Writer: it writes all of p, waiting without bound for
// flow control. A peer that refused the payload fails it, e.g. with
// ErrRejected.
func (t *OutgoingTransfer) Write(p []byte) (int, error) {
	return t.WriteContext(context.Background(), p)
}

// WriteContext is Write, waiting until ctx ends; it returns how much of p
// was written.
func (t *OutgoingTransfer) WriteContext(ctx context.Context, p []byte) (int, error) {
	written := 0
	for written < len(p) {
		rest := p[written:]
		var n C.size_t
		err := wait(ctx, &t.h,
			func(fd *C.int64_t, e *C.weida_go_error) C.int {
				return C.go_outgoing_transfer_fd((*C.weida_outgoing_transfer)(t.h.ptr), fd, e)
			},
			func(e *C.weida_go_error) C.int {
				return t.h.call(func(ptr unsafe.Pointer) C.int {
					return C.go_outgoing_transfer_write((*C.weida_outgoing_transfer)(ptr), bytesIn(rest), C.size_t(len(rest)), &n, e)
				})
			})
		if err != nil {
			return written, err
		}
		written += int(n)
	}
	return written, nil
}

// Finish ends the payload and returns its delivery receipt; it consumes
// the transfer. Fails only with ErrExpired.
func (t *OutgoingTransfer) Finish() (*Delivery, error) {
	var out *C.weida_delivery
	var e C.weida_go_error
	rc := t.h.consume(func(p unsafe.Pointer) C.int {
		return C.go_outgoing_transfer_finish((*C.weida_outgoing_transfer)(p), &out, &e)
	})
	if err := check(rc, &e); err != nil {
		return nil, err
	}
	d := &Delivery{}
	return track(d, &d.h, unsafe.Pointer(out), dropDelivery), nil
}

// Close releases the transfer; an unfinished payload is reset with
// ErrCanceled.
func (t *OutgoingTransfer) Close() error { return t.h.close() }

// Delivery is the receipt of a finished outbound transfer.
type Delivery struct{ h handle }

func dropDelivery(p unsafe.Pointer) { C.weida_delivery_drop((*C.weida_delivery)(p)) }

// Delivered waits until the peer's transport acknowledged the whole
// payload, or returns the refusal or loss that stopped it. It consumes the
// receipt.
func (d *Delivery) Delivered(ctx context.Context) error {
	var op *C.weida_delivery_delivered_op
	var e C.weida_go_error
	rc := d.h.consume(func(p unsafe.Pointer) C.int {
		return C.go_delivery_delivered((*C.weida_delivery)(p), &op, &e)
	})
	if err := check(rc, &e); err != nil {
		return err
	}
	defer C.weida_delivery_delivered_op_drop(op)
	return wait(ctx, nil,
		func(fd *C.int64_t, e *C.weida_go_error) C.int { return C.go_delivery_delivered_op_fd(op, fd, e) },
		func(e *C.weida_go_error) C.int { return C.go_delivery_delivered_op_wait(op, e) })
}

// Close releases the receipt without waiting on it.
func (d *Delivery) Close() error { return d.h.close() }

// TransferMeta is the metadata an outgoing transfer carries in its header.
type TransferMeta struct {
	// ContentType is an opaque label; "" sets none.
	ContentType string
}

// open turns the metadata into a C handle for one call; nil for nil.
func (m *TransferMeta) open() (*C.weida_transfer_meta, error) {
	if m == nil {
		return nil, nil
	}
	var out *C.weida_transfer_meta
	var e C.weida_go_error
	if err := check(C.go_transfer_meta_new(&out, &e), &e); err != nil {
		return nil, err
	}
	if m.ContentType != "" {
		ct, err := cString(m.ContentType)
		if err == nil {
			err = check(C.go_transfer_meta_set_content_type(out, ct, &e), &e)
			freeString(ct)
		}
		if err != nil {
			C.weida_transfer_meta_drop(out)
			return nil, err
		}
	}
	return out, nil
}

// IncomingMeta is the metadata of an inbound transfer or request, copied
// out at once; nil where the value is absent.
type IncomingMeta struct {
	// Endpoint is the path the transfer was sent to; nil on a reply.
	Endpoint *string
	// ContentType is the sender's label.
	ContentType *string
	// Topic is the Pub/Sub topic.
	Topic *string
	// PeerFingerprint is the key the sender proved in the TLS handshake;
	// nil when it dialled anonymously or over a local transport.
	PeerFingerprint *string
	// PeerChainDER is the certificates the sender presented in the TLS
	// handshake, DER, leaf first; nil when it dialled anonymously, over a
	// local transport, or over a bound weida keeps. PeerChain parses it.
	PeerChainDER [][]byte
	// ContentLen is the advisory payload length the sender stated.
	ContentLen *uint64
	// Sequence is the sender's per-producer sequence number.
	Sequence *uint64
}

// PeerChain is the certificates the sender presented, leaf first, parsed
// from PeerChainDER on each call; nil, nil when there is none. A
// certificate that does not parse is ErrTLS.
func (m IncomingMeta) PeerChain() ([]*x509.Certificate, error) {
	if m.PeerChainDER == nil {
		return nil, nil
	}
	certs := make([]*x509.Certificate, 0, len(m.PeerChainDER))
	for i, der := range m.PeerChainDER {
		c, err := x509.ParseCertificate(der)
		if err != nil {
			return nil, &Error{Code: CodeTLS, Message: fmt.Sprintf("certificate %d of the peer's chain does not parse: %v", i, err), Max: -1}
		}
		certs = append(certs, c)
	}
	return certs, nil
}

func copyMeta(m *C.weida_incoming_meta) IncomingMeta {
	var meta IncomingMeta
	var n C.size_t
	str := func(s *C.char) *string {
		if s == nil {
			return nil
		}
		v := C.GoStringN(s, C.int(n))
		return &v
	}
	meta.Endpoint = str(C.weida_incoming_meta_endpoint(m, &n))
	meta.ContentType = str(C.weida_incoming_meta_content_type(m, &n))
	meta.Topic = str(C.weida_incoming_meta_topic(m, &n))
	meta.PeerFingerprint = str(C.weida_incoming_meta_peer_fingerprint(m, &n))
	if C.weida_incoming_meta_peer_chain_len(m, &n) == 1 {
		meta.PeerChainDER = make([][]byte, 0, int(n))
		for i := C.size_t(0); i < n; i++ {
			var l C.size_t
			der := C.weida_incoming_meta_peer_chain_cert(m, i, &l)
			meta.PeerChainDER = append(meta.PeerChainDER, C.GoBytes(unsafe.Pointer(der), C.int(l)))
		}
	}
	var u C.uint64_t
	num := func(present C.int) *uint64 {
		if present != 1 {
			return nil
		}
		v := uint64(u)
		return &v
	}
	meta.ContentLen = num(C.weida_incoming_meta_content_len(m, &u))
	meta.Sequence = num(C.weida_incoming_meta_sequence(m, &u))
	return meta
}

// Message is a collected payload and the metadata of the transfer it
// consumed.
type Message struct {
	Payload []byte
	Meta    IncomingMeta
}
