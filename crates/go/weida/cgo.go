// Package weida is weida for Go: a cgo package over the C ABI libweida
// (docs/decisions/0046-how-each-language-binds-libweida.md), linked
// statically. Its API mirrors the C ABI's current slice
// (docs/libraries/weida-go.md): one type per handle, one method per
// function, a context.Context first on every method that waits.
//
// No call blocks in C: every C call only tries, and a call that would block
// parks its goroutine in Go's netpoller on the handle's readiness
// descriptor until the next try can succeed, its context ends, or the
// handle is closed.
//
// The library directory comes from the caller, e.g.
// CGO_LDFLAGS=-L<dir with libweida.a>; develop.sh builds it and runs the
// tests.
package weida

/*
#cgo CFLAGS: -I${SRCDIR}/../../weida-c/include
#cgo linux LDFLAGS: -lweida -lgcc_s -lutil -lrt -lpthread -lm -ldl -lc
#cgo darwin LDFLAGS: -lweida -framework Security -framework CoreFoundation -liconv -lSystem -lc -lm
#include "weida_go.h"
*/
import "C"

// Version is the version of the linked libweida, e.g.
// "0.1.0-alpha.11-dev".
func Version() string {
	return C.GoString(C.weida_version())
}
