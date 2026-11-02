package weida

// #include "weida_go.h"
import "C"

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"
	"unsafe"
)

// handle is one libweida handle owned by a Go value (docs/c/README.md R5).
//
// A call holds the read lock for the duration of its C call and fails with
// ErrClosed once the handle is closed; Close and the consuming methods
// take the write lock, so no call starts after the C drop (or the consuming
// C call) returned. A waiting goroutine holds no lock while it is parked.
type handle struct {
	mu      sync.RWMutex
	ptr     unsafe.Pointer
	closed  bool
	drop    func(unsafe.Pointer)
	cleanup runtime.Cleanup

	// The descriptors of the goroutines parked on this handle; closing one
	// ends that park.
	wmu     sync.Mutex
	waiting map[*os.File]struct{}
}

// track gives obj's handle the C pointer p and drops p once obj is
// unreachable without having been closed or consumed.
func track[T any](obj *T, h *handle, p unsafe.Pointer, drop func(unsafe.Pointer)) *T {
	h.ptr, h.drop = p, drop
	h.cleanup = runtime.AddCleanup(obj, drop, p)
	return obj
}

// call runs f with the C pointer under the read lock; on a closed handle it
// returns -WEIDA_ECLOSED without calling f.
func (h *handle) call(f func(p unsafe.Pointer) C.int) C.int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.closed {
		return -C.WEIDA_ECLOSED
	}
	return f(h.ptr)
}

// consume marks the handle closed without a drop and runs f, a C function
// that consumes the pointer on every return path (R6).
func (h *handle) consume(f func(p unsafe.Pointer) C.int) C.int {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return -C.WEIDA_ECLOSED
	}
	h.release()
	return f(h.ptr)
}

// close drops the C handle once; every later call fails with ErrClosed and
// every goroutine parked on it returns ErrClosed.
func (h *handle) close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.closed {
		h.release()
		h.drop(h.ptr)
	}
	return nil
}

// release marks the handle closed and ends every park on it. The write lock
// is held.
func (h *handle) release() {
	h.closed = true
	h.cleanup.Stop()
	h.wmu.Lock()
	for f := range h.waiting {
		f.Close()
	}
	h.waiting = nil
	h.wmu.Unlock()
}

func (h *handle) isClosed() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.closed
}

// fdFunc writes a readiness descriptor (a weida_*_fd wrapper).
type fdFunc func(out *C.int64_t, e *C.weida_go_error) C.int

// tryFunc is one non-blocking attempt: a try or an op wait with
// timeout_ms = 0.
type tryFunc func(e *C.weida_go_error) C.int

// wait runs try until it returns something other than -WEIDA_EAGAIN,
// parking the goroutine in the netpoller on the descriptor fd writes in
// between (R11: readable when the next try will not block).
//
// owner, when not nil, is the handle the wait belongs to: closing it ends
// the park with ErrClosed. Each park watches a duplicate of the descriptor
// of its own, so goroutines waiting on one handle neither queue behind each
// other nor share a deadline. The context ends the park through a read
// deadline in the past; the call then returns ctx.Err().
func wait(ctx context.Context, owner *handle, fd fdFunc, try tryFunc) error {
	var e C.weida_go_error
	rc := try(&e)
	if rc != -C.WEIDA_EAGAIN {
		return check(rc, &e)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	f, err := owner.watch(fd)
	if err != nil {
		return err
	}
	defer owner.unwatch(f)
	raw, err := f.SyscallConn()
	if err != nil {
		return err
	}
	stop := context.AfterFunc(ctx, func() { f.SetReadDeadline(time.Unix(1, 0)) })
	defer stop()
	err = raw.Read(func(uintptr) bool {
		rc = try(&e)
		return rc != -C.WEIDA_EAGAIN
	})
	switch {
	case err == nil:
		return check(rc, &e)
	case ctx.Err() != nil:
		return ctx.Err()
	case owner != nil && owner.isClosed():
		return ErrClosed
	default:
		return fmt.Errorf("weida: waiting on a readiness descriptor: %w", err)
	}
}

// watch opens the descriptor fd writes as a file in the netpoller and
// registers it with the handle, unless the handle is closed. On a nil
// handle it only opens the file.
func (h *handle) watch(fd fdFunc) (*os.File, error) {
	if h == nil {
		return readiness(fd)
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.closed {
		return nil, ErrClosed
	}
	f, err := readiness(fd)
	if err != nil {
		return nil, err
	}
	h.wmu.Lock()
	if h.waiting == nil {
		h.waiting = make(map[*os.File]struct{})
	}
	h.waiting[f] = struct{}{}
	h.wmu.Unlock()
	return f, nil
}

func (h *handle) unwatch(f *os.File) {
	if h != nil {
		h.wmu.Lock()
		delete(h.waiting, f)
		h.wmu.Unlock()
	}
	f.Close()
}

// readiness duplicates a readiness descriptor into an *os.File. libweida's
// descriptor is non-blocking (R11), and so is the duplicate, which makes
// os.NewFile register it with the netpoller.
func readiness(fd fdFunc) (*os.File, error) {
	var raw C.int64_t
	var e C.weida_go_error
	if err := check(fd(&raw, &e), &e); err != nil {
		return nil, err
	}
	dup, err := C.weida_go_dup(raw)
	if dup < 0 {
		return nil, fmt.Errorf("weida: duplicating a readiness descriptor: %w", err)
	}
	return os.NewFile(uintptr(dup), "weida-readiness"), nil
}

// cString copies s for C; a string with a NUL in it is ErrInval, which C
// would otherwise see cut short.
func cString(s string) (*C.char, error) {
	if strings.IndexByte(s, 0) >= 0 {
		return nil, ErrInval
	}
	return C.CString(s), nil
}

func freeString(s *C.char) {
	C.free(unsafe.Pointer(s))
}

// bytesIn is p for a C call that only reads it for that call (the cgo
// pointer rules allow it: C keeps no Go pointer).
func bytesIn(p []byte) *C.uint8_t {
	return (*C.uint8_t)(unsafe.Pointer(unsafe.SliceData(p)))
}
