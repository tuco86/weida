package weida

// The unit tier: status codes, the failures that need no peer, and Req/Rep
// over the in-process transport, each test on a bus of its own. Every wait
// that should succeed runs on t.Context() without a deadline: go test's
// -timeout is the hang guard.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestEveryCodeMatchesItsSentinel(t *testing.T) {
	sentinels := []struct {
		code Code
		err  *Error
	}{
		{CodeRuntime, ErrRuntime},
		{CodeInvalidAddress, ErrInvalidAddress},
		{CodeInvalidEndpointPath, ErrInvalidEndpointPath},
		{CodeInvalidFingerprint, ErrInvalidFingerprint},
		{CodeAlreadyRegistered, ErrAlreadyRegistered},
		{CodeNotConnected, ErrNotConnected},
		{CodeConnectionLost, ErrConnectionLost},
		{CodeNegotiation, ErrNegotiation},
		{CodeProtocol, ErrProtocol},
		{CodeRejected, ErrRejected},
		{CodeUnknownEndpoint, ErrUnknownEndpoint},
		{CodeUnsupported, ErrUnsupported},
		{CodeNoParkedConnection, ErrNoParkedConnection},
		{CodeNoReply, ErrNoReply},
		{CodeCanceled, ErrCanceled},
		{CodeExpired, ErrExpired},
		{CodeIndeterminate, ErrIndeterminate},
		{CodeLimitExceeded, ErrLimitExceeded},
		{CodeDatagramsUnavailable, ErrDatagramsUnavailable},
		{CodeTooLarge, ErrTooLarge},
		{CodeTLS, ErrTLS},
		{CodeUntrusted, ErrUntrusted},
		{CodeIO, ErrIO},
		{CodeTransport, ErrTransport},
		{CodeAgain, ErrAgain},
		{CodeIntr, ErrIntr},
		{CodeInval, ErrInval},
		{CodeClosed, ErrClosed},
		{CodePanic, ErrPanic},
	}
	for _, s := range sentinels {
		if s.err.Code != s.code {
			t.Errorf("the sentinel of %d has code %d", s.code, s.err.Code)
		}
		if s.code.String() == "unknown error" || s.err.Message != s.code.String() {
			t.Errorf("code %d: description %q, sentinel message %q", s.code, s.code.String(), s.err.Message)
		}
		failure := fmt.Errorf("wrapped: %w", &Error{Code: s.code, Message: "detail"})
		for _, other := range sentinels {
			if got := errors.Is(failure, other.err); got != (other.code == s.code) {
				t.Errorf("errors.Is(code %d, sentinel of %d) = %v", s.code, other.code, got)
			}
		}
	}
}

func TestAnUnknownCodeIsAnUnknownError(t *testing.T) {
	if got := Code(9999).String(); got != "unknown error" {
		t.Fatalf("Code(9999).String() = %q", got)
	}
}

func TestARuntimeWithNoWorkerThreadsIsARuntimeFailure(t *testing.T) {
	_, err := NewRuntime(&RuntimeConfig{WorkerThreads: 0, ShutdownTimeout: time.Second})
	var failure *Error
	if !errors.As(err, &failure) || !errors.Is(err, ErrRuntime) || failure.Message == "" {
		t.Fatalf("NewRuntime with no worker threads: %v", err)
	}
}

func TestAMalformedPinIsAnInvalidFingerprint(t *testing.T) {
	trust := must(NewTrust())(t)
	defer trust.Close()
	if err := trust.AddPin("nope"); !errors.Is(err, ErrInvalidFingerprint) {
		t.Fatalf("AddPin(\"nope\") = %v", err)
	}
}

func TestAStreamedRoundTripCarriesTheBodyAndTheContentType(t *testing.T) {
	p := newPair(t)
	served := async(func() ([]byte, error) {
		return serve(t.Context(), p.replier, bytes.ToUpper, &TransferMeta{ContentType: "text/plain"})
	})
	reply := must(p.requester.Request(t.Context(), []byte("ping")))(t)
	meta := must(reply.Meta())(t)
	if meta.ContentType == nil || *meta.ContentType != "text/plain" {
		t.Errorf("reply content type %v", meta.ContentType)
	}
	msg := must(reply.Collect(t.Context(), 1024))(t)
	if string(msg.Payload) != "PING" {
		t.Errorf("reply payload %q", msg.Payload)
	}
	if msg.Meta.ContentType == nil || *msg.Meta.ContentType != "text/plain" || msg.Meta.Endpoint != nil {
		t.Errorf("message meta %+v", msg.Meta)
	}
	if body := must(await(served))(t); string(body) != "ping" {
		t.Errorf("served body %q", body)
	}
	p.close(t)
}

func TestReadsOf3BytesReassembleTheBody(t *testing.T) {
	p := newPair(t)
	client := requestQuietly(t.Context(), p.requester, []byte("0123456789"))
	request := must(p.replier.Accept(t.Context()))(t)
	meta := must(request.Meta())(t)
	if meta.Endpoint == nil || *meta.Endpoint != "/echo" {
		t.Errorf("request endpoint %v", meta.Endpoint)
	}
	body := must(request.TakeBody())(t)
	if got := must(readAll(t.Context(), body, 3))(t); string(got) != "0123456789" {
		t.Errorf("body %q", got)
	}
	if n, err := body.Read(make([]byte, 3)); n != 0 || err != io.EOF {
		t.Errorf("read past the end: %d, %v", n, err)
	}
	request.Close()
	await(client)
	p.close(t)
}

func TestACollectOverItsCeilingIsLimitExceeded(t *testing.T) {
	p := newPair(t)
	client := requestQuietly(t.Context(), p.requester, []byte("four"))
	request := must(p.replier.Accept(t.Context()))(t)
	body := must(request.TakeBody())(t)
	if _, err := body.Collect(t.Context(), 2); !errors.Is(err, ErrLimitExceeded) {
		t.Errorf("Collect(2) of 4 bytes = %v", err)
	}
	request.Close()
	await(client)
	p.close(t)
}

func TestAReplyDroppedUnfinishedReadsAsCanceled(t *testing.T) {
	p := newPair(t)
	server := async(func() (struct{}, error) {
		request, err := p.replier.Accept(t.Context())
		if err != nil {
			return struct{}{}, err
		}
		reply, err := request.Reply(t.Context(), nil)
		if err != nil {
			return struct{}{}, err
		}
		_, err = reply.WriteContext(t.Context(), []byte("ha"))
		reply.Close()
		return struct{}{}, err
	})
	reply := must(p.requester.Request(t.Context(), []byte("go")))(t)
	if _, err := io.ReadAll(reply); !errors.Is(err, ErrCanceled) {
		t.Errorf("reading a reply dropped unfinished: %v", err)
	}
	must(await(server))(t)
	p.close(t)
}

func TestASecondReplyIsClosed(t *testing.T) {
	p := newPair(t)
	client := requestQuietly(t.Context(), p.requester, []byte("once"))
	request := must(p.replier.Accept(t.Context()))(t)
	reply := must(request.Reply(t.Context(), nil))(t)
	reply.Close()
	if _, err := request.Reply(t.Context(), nil); !errors.Is(err, ErrClosed) {
		t.Errorf("second Reply = %v", err)
	}
	await(client)
	p.close(t)
}

func TestTwoGoroutinesAcceptingOnOneReplierBothGetARequest(t *testing.T) {
	p := newPair(t)
	reverse := func(b []byte) []byte {
		r := slices.Clone(b)
		slices.Reverse(r)
		return r
	}
	accepts := []<-chan result[[]byte]{
		async(func() ([]byte, error) { return serve(t.Context(), p.replier, reverse, nil) }),
		async(func() ([]byte, error) { return serve(t.Context(), p.replier, reverse, nil) }),
	}
	untilParked(t, "(*Replier).Accept", 2)
	var replies []<-chan result[[]byte]
	for _, body := range []string{"one", "two"} {
		replies = append(replies, async(func() ([]byte, error) {
			reply, err := p.requester.Request(t.Context(), []byte(body))
			if err != nil {
				return nil, err
			}
			msg, err := reply.Collect(t.Context(), 1024)
			if err != nil {
				return nil, err
			}
			return msg.Payload, nil
		}))
	}
	var answered, served []string
	for i := range 2 {
		answered = append(answered, string(must(await(replies[i]))(t)))
		served = append(served, string(must(await(accepts[i]))(t)))
	}
	slices.Sort(answered)
	slices.Sort(served)
	if !slices.Equal(answered, []string{"eno", "owt"}) || !slices.Equal(served, []string{"one", "two"}) {
		t.Errorf("answered %q, served %q", answered, served)
	}
	p.close(t)
}

func TestARequestWhoseContextExpiresLeavesTheRuntimeAbleToShutDown(t *testing.T) {
	p := newPair(t)
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if _, err := p.requester.Request(ctx, []byte("unanswered")); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("an unanswered request: %v", err)
	}
	p.close(t)
}

func TestClosingAReplierEndsAPendingAcceptWithClosed(t *testing.T) {
	p := newPair(t)
	accept := async(func() (*IncomingRequest, error) { return p.replier.Accept(t.Context()) })
	untilParked(t, "(*Replier).Accept", 1)
	p.replier.Close()
	if _, err := await(accept); !errors.Is(err, ErrClosed) {
		t.Errorf("a pending Accept after Close: %v", err)
	}
	p.close(t)
}

// ---- helpers ----

// pair is a runtime with a replier at /echo and a requester connected to
// it over a bus of its own.
type pair struct {
	runtime   *Runtime
	listener  *Listener
	binding   *LocalBinding
	replier   *Replier
	requester *Requester
}

var buses atomic.Int64

func newPair(t *testing.T) *pair {
	t.Helper()
	bus := fmt.Sprintf("go-test-%d-%d", os.Getpid(), buses.Add(1))
	p := &pair{}
	p.runtime = must(NewRuntime(nil))(t)
	p.listener = must(p.runtime.Listener())(t)
	p.binding = must(p.listener.BindInproc(bus))(t)
	if got := p.binding.Bus(); got != bus {
		t.Fatalf("Bus() = %q, want %q", got, bus)
	}
	p.replier = must(p.listener.Replier("/echo"))(t)
	if got := p.replier.Path(); got != "/echo" {
		t.Fatalf("Path() = %q", got)
	}
	trust := must(NewTrust())(t)
	defer trust.Close()
	tls := must(NewClientTLS(trust))(t)
	defer tls.Close()
	p.requester = must(p.runtime.Requester(tls))(t)
	if err := p.requester.Connect(t.Context(), "weida+inproc://"+bus+"/echo"); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	return p
}

// close releases the pair and shuts its runtime down.
func (p *pair) close(t *testing.T) {
	t.Helper()
	p.requester.Close()
	p.replier.Close()
	p.binding.Close()
	p.listener.Close()
	if err := p.runtime.Shutdown(t.Context()); err != nil {
		t.Errorf("Shutdown: %v", err)
	}
}

// serve answers one request with answer(body), reading the body 3 bytes at
// a time; it returns the body.
func serve(ctx context.Context, r *Replier, answer func([]byte) []byte, meta *TransferMeta) ([]byte, error) {
	request, err := r.Accept(ctx)
	if err != nil {
		return nil, err
	}
	defer request.Close()
	body, err := request.TakeBody()
	if err != nil {
		return nil, err
	}
	received, err := readAll(ctx, body, 3)
	if err != nil {
		return nil, err
	}
	reply, err := request.Reply(ctx, meta)
	if err != nil {
		return nil, err
	}
	defer reply.Close()
	if _, err := reply.WriteContext(ctx, answer(received)); err != nil {
		return nil, err
	}
	delivery, err := reply.Finish()
	if err != nil {
		return nil, err
	}
	return received, delivery.Delivered(ctx)
}

// readAll reads t to its end in reads of at most size bytes.
func readAll(ctx context.Context, t *IncomingTransfer, size int) ([]byte, error) {
	var all []byte
	buf := make([]byte, size)
	for {
		n, err := t.ReadContext(ctx, buf)
		if err == io.EOF {
			return all, nil
		}
		if err != nil {
			return nil, err
		}
		if n > size {
			return nil, fmt.Errorf("read %d bytes into %d", n, size)
		}
		all = append(all, buf[:n]...)
	}
}

// requestQuietly sends a request whose answer the test does not look at.
func requestQuietly(ctx context.Context, r *Requester, body []byte) <-chan result[struct{}] {
	return async(func() (struct{}, error) {
		reply, err := r.Request(ctx, body)
		if err == nil {
			_, err = reply.Collect(ctx, 1024)
		}
		return struct{}{}, err
	})
}

type result[T any] struct {
	value T
	err   error
}

func async[T any](f func() (T, error)) <-chan result[T] {
	c := make(chan result[T], 1)
	go func() {
		v, err := f()
		c <- result[T]{v, err}
	}()
	return c
}

func await[T any](c <-chan result[T]) (T, error) {
	r := <-c
	return r.value, r.err
}

// must fails the test on an error and returns the value otherwise:
// must(f())(t).
func must[T any](v T, err error) func(*testing.T) T {
	return func(t *testing.T) T {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
}

// untilParked waits until n goroutines are parked in the netpoller inside
// wait, called from the function named caller.
func untilParked(t *testing.T, caller string, n int) {
	t.Helper()
	buf := make([]byte, 1<<20)
	for {
		stacks := string(buf[:runtime.Stack(buf, true)])
		parked := 0
		for _, g := range strings.Split(stacks, "\n\n") {
			if strings.Contains(g, "[IO wait") && strings.Contains(g, "/weida.wait(") && strings.Contains(g, caller) {
				parked++
			}
		}
		if parked >= n {
			return
		}
		runtime.Gosched()
	}
}
