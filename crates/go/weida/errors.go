package weida

// #include "weida_go.h"
import "C"

import "unsafe"

// Code is a status code of the C ABI (docs/c/README.md R9), positive.
type Code int

// The 24 variants of weida's error, in declaration order, and the ABI's own
// five codes.
const (
	CodeRuntime              Code = C.WEIDA_ERUNTIME
	CodeInvalidAddress       Code = C.WEIDA_EINVALID_ADDRESS
	CodeInvalidEndpointPath  Code = C.WEIDA_EINVALID_ENDPOINT_PATH
	CodeInvalidFingerprint   Code = C.WEIDA_EINVALID_FINGERPRINT
	CodeAlreadyRegistered    Code = C.WEIDA_EALREADY_REGISTERED
	CodeNotConnected         Code = C.WEIDA_ENOT_CONNECTED
	CodeConnectionLost       Code = C.WEIDA_ECONNECTION_LOST
	CodeNegotiation          Code = C.WEIDA_ENEGOTIATION
	CodeProtocol             Code = C.WEIDA_EPROTOCOL
	CodeRejected             Code = C.WEIDA_EREJECTED
	CodeUnknownEndpoint      Code = C.WEIDA_EUNKNOWN_ENDPOINT
	CodeUnsupported          Code = C.WEIDA_EUNSUPPORTED
	CodeNoParkedConnection   Code = C.WEIDA_ENO_PARKED_CONNECTION
	CodeNoReply              Code = C.WEIDA_ENO_REPLY
	CodeCanceled             Code = C.WEIDA_ECANCELED
	CodeExpired              Code = C.WEIDA_EEXPIRED
	CodeIndeterminate        Code = C.WEIDA_EINDETERMINATE
	CodeLimitExceeded        Code = C.WEIDA_ELIMIT_EXCEEDED
	CodeDatagramsUnavailable Code = C.WEIDA_EDATAGRAMS_UNAVAILABLE
	CodeTooLarge             Code = C.WEIDA_ETOO_LARGE
	CodeTLS                  Code = C.WEIDA_ETLS
	CodeUntrusted            Code = C.WEIDA_EUNTRUSTED
	CodeIO                   Code = C.WEIDA_EIO
	CodeTransport            Code = C.WEIDA_ETRANSPORT

	CodeAgain  Code = C.WEIDA_EAGAIN
	CodeIntr   Code = C.WEIDA_EINTR
	CodeInval  Code = C.WEIDA_EINVAL
	CodeClosed Code = C.WEIDA_ECLOSED
	CodePanic  Code = C.WEIDA_EPANIC
)

// String is the code's description (weida_strerror), "unknown error" for a
// code the library does not define.
func (c Code) String() string {
	return C.GoString(C.weida_strerror(C.int(c)))
}

// LossCause says why a connection was lost (CodeConnectionLost); 0 when the
// error is another one.
type LossCause int

// The causes of CodeConnectionLost.
const (
	LossIdleTimeout    LossCause = C.WEIDA_LOSS_IDLE_TIMEOUT
	LossPeerClosed     LossCause = C.WEIDA_LOSS_PEER_CLOSED
	LossPeerRestarting LossCause = C.WEIDA_LOSS_PEER_RESTARTING
	LossLocallyClosed  LossCause = C.WEIDA_LOSS_LOCALLY_CLOSED
	LossReset          LossCause = C.WEIDA_LOSS_RESET
	LossTransportError LossCause = C.WEIDA_LOSS_TRANSPORT_ERROR
)

// Error is a failure of libweida, with the record the failing call left.
//
// errors.Is matches it against the sentinel of its code:
// errors.Is(err, ErrUntrusted).
type Error struct {
	Code Code
	// Message is the Display text of weida's error, or the code's
	// description when the failure has none.
	Message string
	// LossCause is set when Code is CodeConnectionLost.
	LossCause LossCause
	// Fingerprint is the key the peer presented when Code is CodeUntrusted.
	Fingerprint string
	// Max is the largest payload the connection carries when Code is
	// CodeTooLarge, otherwise -1.
	Max int64
	// OSCode is the raw OS error when Code is CodeIO and it carried one.
	OSCode int
}

func (e *Error) Error() string {
	return "weida: " + e.Message
}

// Is reports whether target is an *Error with the same code.
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	return ok && t.Code == e.Code
}

// One sentinel per code, for errors.Is.
var (
	ErrRuntime              = sentinel(CodeRuntime)
	ErrInvalidAddress       = sentinel(CodeInvalidAddress)
	ErrInvalidEndpointPath  = sentinel(CodeInvalidEndpointPath)
	ErrInvalidFingerprint   = sentinel(CodeInvalidFingerprint)
	ErrAlreadyRegistered    = sentinel(CodeAlreadyRegistered)
	ErrNotConnected         = sentinel(CodeNotConnected)
	ErrConnectionLost       = sentinel(CodeConnectionLost)
	ErrNegotiation          = sentinel(CodeNegotiation)
	ErrProtocol             = sentinel(CodeProtocol)
	ErrRejected             = sentinel(CodeRejected)
	ErrUnknownEndpoint      = sentinel(CodeUnknownEndpoint)
	ErrUnsupported          = sentinel(CodeUnsupported)
	ErrNoParkedConnection   = sentinel(CodeNoParkedConnection)
	ErrNoReply              = sentinel(CodeNoReply)
	ErrCanceled             = sentinel(CodeCanceled)
	ErrExpired              = sentinel(CodeExpired)
	ErrIndeterminate        = sentinel(CodeIndeterminate)
	ErrLimitExceeded        = sentinel(CodeLimitExceeded)
	ErrDatagramsUnavailable = sentinel(CodeDatagramsUnavailable)
	ErrTooLarge             = sentinel(CodeTooLarge)
	ErrTLS                  = sentinel(CodeTLS)
	ErrUntrusted            = sentinel(CodeUntrusted)
	ErrIO                   = sentinel(CodeIO)
	ErrTransport            = sentinel(CodeTransport)

	ErrAgain  = sentinel(CodeAgain)
	ErrIntr   = sentinel(CodeIntr)
	ErrInval  = sentinel(CodeInval)
	ErrClosed = sentinel(CodeClosed)
	ErrPanic  = sentinel(CodePanic)
)

func sentinel(c Code) *Error {
	return &Error{Code: c, Message: c.String(), Max: -1}
}

// check turns a status into an error, taking the record e holds when it is
// the record of this failure.
func check(rc C.int, e *C.weida_go_error) error {
	if rc >= 0 {
		return nil
	}
	code := Code(-rc)
	err := &Error{Code: code, Max: -1}
	if Code(e.code) == code {
		err.LossCause = LossCause(e.loss_cause)
		err.Max = int64(e.max)
		err.OSCode = int(e.os_code)
		if e.message != nil {
			err.Message = C.GoString(e.message)
			C.free(unsafe.Pointer(e.message))
			e.message = nil
		}
		if e.fingerprint != nil {
			err.Fingerprint = C.GoString(e.fingerprint)
			C.free(unsafe.Pointer(e.fingerprint))
			e.fingerprint = nil
		}
	}
	if err.Message == "" {
		err.Message = code.String()
	}
	return err
}
