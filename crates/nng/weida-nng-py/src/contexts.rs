//! `nng_ctx` as a Python object, for the four protocols that have one.
//!
//! "Several requests can be processed in parallel over one socket" is what
//! a context is for (`docs/research/nanomsg-nng.md` §4): the socket carries
//! the pipes, and each context carries one transaction's state — its own
//! request or survey id, its own deadline, and its own `NNG_ESTATE` rule.
//! Without them a REQ socket is one transaction at a time, which is the
//! whole reason NNG grew contexts.
//!
//! So they are objects here rather than a flag: `socket.context()` hands
//! out a new one, several coroutines each hold their own, and the
//! transactions overlap. A context dropped by Python is removed from its
//! socket, which is `nng_ctx_close`.
//!
//! REP and RESPONDENT share one class because they share one machine: both
//! receive a message whose body begins with a tag stack, both remember the
//! stack, and both may send only the answer to what they received. The
//! library's `RepCtx` and `RespondentCtx` are one Rust type for the same
//! reason.

use std::sync::Arc;

use pyo3::prelude::*;
use weida_py_core::Bridge;

use crate::errors::errno_of;

/// Builds one context class over one of the library's context types.
macro_rules! python_context {
    ($class:ident, $rust:ty, $python:literal, $what:literal, [$($flag:ident)*]) => {
        python_context!(@build $class, $rust, $python, $what, {}, [$($flag)*]);
    };

    (@build $class:ident, $rust:ty, $python:literal, $what:literal,
     {$($acc:tt)*}, [send $($rest:ident)*]) => {
        python_context!(@build $class, $rust, $python, $what, {
            $($acc)*

            /// Sends this context's message: the request, the survey, or
            /// the answer to what this context received.
            ///
            /// Out of turn — an answer nobody asked for, a second request
            /// before the first was answered — is `ESTATE`, which is the
            /// protocol's own refusal and is per context rather than per
            /// socket.
            fn send<'py>(
                &self,
                py: Python<'py>,
                body: &Bound<'py, PyAny>,
            ) -> PyResult<Bound<'py, PyAny>> {
                let body = weida_py_core::payload_of(body)?;
                let context = Arc::clone(&self.context);
                self.bridge.awaitable(py, async move {
                    context.send(body).await.map_err(errno_of)
                })
            }
        }, [$($rest)*]);
    };

    (@build $class:ident, $rust:ty, $python:literal, $what:literal,
     {$($acc:tt)*}, [recv $($rest:ident)*]) => {
        python_context!(@build $class, $rust, $python, $what, {
            $($acc)*

            /// Receives this context's message, bounded by the socket's
            /// receive timeout — and, for a surveyor, by the deadline its
            /// own send started.
            ///
            /// A message this receive had taken when the task was cancelled
            /// is dropped, not handed back. It is a reply, request or survey
            /// response, and it belongs to the transaction whose state this
            /// receive has already advanced; re-delivering it later would
            /// pair it with the wrong state. The loss is the protocol's own
            /// at-most-once delivery, recovered by the resend timer of REQ
            /// or the survey deadline.
            fn recv<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
                let context = Arc::clone(&self.context);
                self.bridge.awaitable(py, async move {
                    context
                        .recv()
                        .await
                        .map(crate::values::Body)
                        .map_err(errno_of)
                })
            }
        }, [$($rest)*]);
    };

    (@build $class:ident, $rust:ty, $python:literal, $what:literal,
     {$($pattern:tt)*}, []) => {
        #[doc = $what]
        #[pyclass(frozen, name = $python, module = "weida_nng")]
        pub struct $class {
            context: Arc<$rust>,
            bridge: Bridge,
        }

        impl $class {
            /// Wraps one of the library's contexts.
            pub(crate) fn wrap(context: $rust, bridge: Bridge) -> $class {
                $class {
                    context: Arc::new(context),
                    bridge,
                }
            }
        }

        #[pymethods]
        impl $class {
            /// `nng_ctx_id()`: this context's number within its socket.
            #[getter]
            fn id(&self) -> u64 {
                self.context.id().get()
            }

            fn __repr__(&self) -> String {
                format!(
                    concat!("<weida_nng.", $python, " id={}>"),
                    self.context.id().get()
                )
            }

            $($pattern)*
        }
    };
}

python_context!(
    PyReqContext,
    weida_nng::ReqCtx,
    "ReqContext",
    "One REQ transaction: send a request, then receive its reply.\n\nThe \
     request id is this context's, so two contexts on one socket may have \
     two requests outstanding and each reply finds its own.",
    [send recv]
);

python_context!(
    PyReplyContext,
    weida_nng::RepCtx,
    "ReplyContext",
    "One REP or RESPONDENT transaction: receive, then answer what was \
     received.\n\nThe two protocols are one machine and therefore one class; \
     what differs is the id in the header and which socket handed the \
     context out.",
    [recv send]
);

python_context!(
    PySurveyContext,
    weida_nng::SurveyorCtx,
    "SurveyContext",
    "One survey: broadcast, then collect until this context's own deadline \
     expires with `ETIMEDOUT`.\n\nThe deadline starts at the send, so two \
     contexts surveying at different moments expire at different moments.",
    [send recv]
);
