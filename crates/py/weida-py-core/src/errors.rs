//! An errno vocabulary, as Python exception classes.
//!
//! A C messaging library reports failure as `errno`, and every Rust library in
//! this workspace keeps that vocabulary rather than inventing one — libzmq's
//! `EAGAIN`/`EFSM`/`ETERM`, NNG's `NNG_E*`, MQTT's reason codes. A binding that
//! folded them into `RuntimeError` would throw the branch away: the zguide's
//! Lazy Pirate *is* a branch on a timeout, and `ZMQ_DONTWAIT` *is* a branch on
//! `EAGAIN`. So each name becomes its own exception class:
//!
//! ```text
//! Exception
//!  └── weida_zmq.ZmqError          # the base: `except ZmqError` catches all
//!       ├── weida_zmq.EAGAIN       # one class per errno name
//!       ├── weida_zmq.EFSM
//!       └── ...
//! ```
//!
//! and every instance carries `errno` (the name) and `cause` (why), because
//! `errno` alone is a number and a caller that wants to know *which* option was
//! refused should not have to guess.
//!
//! **The classes are built at import, not per error.** An exception class is a
//! Python type object; creating one per raise would be absurd, and looking one
//! up by string in a dict per raise is what [`ErrorFamily`] does — one hash of
//! a short static string against a map built once.

use std::collections::HashMap;
use std::ffi::CString;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyString, PyType};

/// One failure, in the library's own errno vocabulary.
///
/// This is the *only* error type the bridge understands, and it is protocol-
/// neutral on purpose: a binding maps its library's error enum to one of these
/// at the call site, with a free function, because neither the enum nor the
/// trait would be local to the binding crate and Rust's orphan rule would
/// refuse the `impl`.
///
/// ```
/// use weida_py_core::Errno;
///
/// let errno = Errno::new("EAGAIN", "no peer took the request (ZMQ_SNDTIMEO)");
/// assert_eq!(errno.name(), "EAGAIN");
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Errno {
    name: &'static str,
    cause: String,
}

impl Errno {
    /// A failure named `name`, because of `cause`.
    ///
    /// `name` is `&'static str` rather than `String`: an errno name is a
    /// compile-time constant of the library that reports it — never remote
    /// input — and a static one costs no allocation on a path that exists to
    /// report a failure.
    pub fn new(name: &'static str, cause: impl Into<String>) -> Errno {
        Errno {
            name,
            cause: cause.into(),
        }
    }

    /// The errno name, which is also the Python class name.
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// Why it failed, in words.
    pub fn cause(&self) -> &str {
        &self.cause
    }
}

impl std::fmt::Display for Errno {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.name, self.cause)
    }
}

/// Turns an [`Errno`] into the Python exception a module raises for it.
///
/// A plain function pointer, so that a [`Bridge`](crate::Bridge) can hold one
/// without a generic parameter, a lifetime or an allocation: the mapper a
/// binding passes is `fn(Python, &Errno) -> PyErr` and resolves its own
/// [`ErrorClasses`] static.
pub type ErrnoMapper = fn(Python<'_>, &Errno) -> PyErr;

/// One base class and one subclass per errno name, created once.
pub struct ErrorFamily {
    base: Py<PyType>,
    classes: HashMap<&'static str, Py<PyType>>,
    base_name: String,
}

impl ErrorFamily {
    /// Creates the base class `module.base` and one subclass per name.
    ///
    /// `module` is the dotted module name the classes belong to
    /// (`weida_zmq`), which is what puts `weida_zmq.EAGAIN` in a traceback
    /// rather than a bare `EAGAIN` of unknown origin.
    ///
    /// A duplicate name is [`PyValueError`](pyo3::exceptions::PyValueError):
    /// two classes of one name would mean one of them is unreachable, and a
    /// binding whose table drifted should hear about it at import.
    pub fn build(
        py: Python<'_>,
        module: &str,
        base: &str,
        names: &[&'static str],
    ) -> PyResult<ErrorFamily> {
        let base_name = format!("{module}.{base}");
        let base_class = PyErr::new_type(py, &cstring(&base_name)?, None, None, None)?;
        let mut classes = HashMap::with_capacity(names.len());
        for name in names {
            let class = PyErr::new_type(
                py,
                &cstring(&format!("{module}.{name}"))?,
                None,
                Some(base_class.bind(py)),
                None,
            )?;
            if classes.insert(*name, class).is_some() {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "the errno name {name} is listed twice for {base_name}"
                )));
            }
        }
        Ok(ErrorFamily {
            base: base_class,
            classes,
            base_name,
        })
    }

    /// The base class every member of this family derives from.
    pub fn base<'py>(&self, py: Python<'py>) -> &Bound<'py, PyType> {
        self.base.bind(py)
    }

    /// The class for one errno name, or `None` if the family has no such name.
    pub fn class<'py>(&self, py: Python<'py>, name: &str) -> Option<&Bound<'py, PyType>> {
        self.classes.get(name).map(|class| class.bind(py))
    }

    /// Every errno name this family has a class for, unordered.
    pub fn names(&self) -> impl ExactSizeIterator<Item = &'static str> + '_ {
        self.classes.keys().copied()
    }

    /// Adds the base class and every subclass to `module` under its own name.
    ///
    /// After this, `from weida_zmq import EAGAIN` works, and so does
    /// `except weida_zmq.ZmqError`.
    pub fn register(&self, module: &Bound<'_, PyModule>) -> PyResult<()> {
        module.add(
            self.base_name.rsplit('.').next().unwrap_or("Error"),
            &self.base,
        )?;
        for (name, class) in &self.classes {
            module.add(*name, class)?;
        }
        Ok(())
    }

    /// The exception for `errno`: its own class, its cause as the message, and
    /// `errno` and `cause` readable as attributes.
    ///
    /// An unknown name — a variant the binding forgot to list — raises the
    /// *base* class with the name in the message rather than panicking or
    /// silently dropping it: a binding's table may lag its library by one
    /// release, and a caller must still see which errno arrived.
    pub fn error(&self, py: Python<'_>, errno: &Errno) -> PyErr {
        let class = self.classes.get(errno.name()).unwrap_or(&self.base);
        // Instantiating an exception can only fail if the class or the
        // interpreter is broken; reporting *that* as an error beats
        // panicking inside a `raise`.
        instantiate(py, class.bind(py), errno).unwrap_or_else(|err| err)
    }
}

impl std::fmt::Debug for ErrorFamily {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ErrorFamily")
            .field("base", &self.base_name)
            .field("classes", &self.classes.len())
            .finish()
    }
}

/// Builds the exception instance and hangs the errno name off it.
fn instantiate(py: Python<'_>, class: &Bound<'_, PyType>, errno: &Errno) -> PyResult<PyErr> {
    let instance = class.call1((errno.cause(),))?;
    instance.setattr("errno", PyString::new(py, errno.name()))?;
    instance.setattr("cause", PyString::new(py, errno.cause()))?;
    Ok(PyErr::from_value(instance))
}

/// A `CString` for PyO3's type-name arguments, refusing an interior NUL rather
/// than truncating a class name at it.
fn cstring(name: &str) -> PyResult<CString> {
    CString::new(name).map_err(|_| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "an exception class name may not contain a NUL byte: {name:?}"
        ))
    })
}

/// A module's [`ErrorFamily`], created at import and read on every raise.
///
/// Declared as a `static` in the binding, which is what makes
/// [`ErrnoMapper`] a plain function pointer:
///
/// ```no_run
/// use pyo3::prelude::*;
/// use weida_py_core::{Errno, ErrorClasses};
///
/// static ERRORS: ErrorClasses = ErrorClasses::new();
///
/// fn to_py(py: Python<'_>, errno: &Errno) -> PyErr {
///     ERRORS.error(py, errno)
/// }
/// ```
pub struct ErrorClasses {
    family: PyOnceLock<ErrorFamily>,
}

impl ErrorClasses {
    /// An empty cell, for a `static`.
    pub const fn new() -> ErrorClasses {
        ErrorClasses {
            family: PyOnceLock::new(),
        }
    }

    /// Builds the family for `module` and registers every class on it.
    ///
    /// Called once, from the `#[pymodule]` initialiser. A second call on the
    /// same cell registers the classes already built rather than building a
    /// second set, because two type objects of one name in one interpreter
    /// would make `except` depend on which one a value came from — that is the
    /// bug a sub-interpreter or a re-import would otherwise introduce.
    pub fn install(
        &self,
        module: &Bound<'_, PyModule>,
        base: &str,
        names: &[&'static str],
    ) -> PyResult<()> {
        let py = module.py();
        let module_name = module.name()?.extract::<String>()?;
        let family = self
            .family
            .get_or_try_init(py, || ErrorFamily::build(py, &module_name, base, names))?;
        family.register(module)
    }

    /// The family, if [`install`](ErrorClasses::install) has run.
    pub fn family(&self, py: Python<'_>) -> Option<&ErrorFamily> {
        self.family.get(py)
    }

    /// The exception for `errno`.
    ///
    /// Before `install` — which can only happen if a binding calls into the
    /// bridge from outside its own module initialisation — this is a
    /// `RuntimeError` naming the errno, because a missing exception table must
    /// not turn a reportable failure into a panic.
    pub fn error(&self, py: Python<'_>, errno: &Errno) -> PyErr {
        match self.family.get(py) {
            Some(family) => family.error(py, errno),
            None => PyRuntimeError::new_err(format!(
                "{errno} (this module's exception classes are not installed)"
            )),
        }
    }
}

impl Default for ErrorClasses {
    fn default() -> ErrorClasses {
        ErrorClasses::new()
    }
}

impl std::fmt::Debug for ErrorClasses {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ErrorClasses").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn family(py: Python<'_>) -> ErrorFamily {
        ErrorFamily::build(py, "demo", "DemoError", &["EAGAIN", "ETERM"]).expect("family builds")
    }

    #[test]
    fn every_name_is_its_own_class_under_one_base() {
        Python::initialize();
        Python::attach(|py| {
            let family = family(py);
            let eagain = family.class(py, "EAGAIN").expect("EAGAIN exists");
            let eterm = family.class(py, "ETERM").expect("ETERM exists");
            assert!(!eagain.is(eterm), "two errnos must not share a class");
            assert!(eagain.is_subclass(family.base(py)).expect("subclass check"));
            assert!(eterm.is_subclass(family.base(py)).expect("subclass check"));
            assert!(family.class(py, "ENOENT").is_none());
        });
    }

    #[test]
    fn the_exception_carries_its_errno_name_and_cause() {
        Python::initialize();
        Python::attach(|py| {
            let family = family(py);
            let err = family.error(py, &Errno::new("EAGAIN", "would block"));
            let value = err.value(py);
            assert_eq!(
                value.getattr("errno").unwrap().extract::<String>().unwrap(),
                "EAGAIN"
            );
            assert_eq!(
                value.getattr("cause").unwrap().extract::<String>().unwrap(),
                "would block"
            );
            assert_eq!(value.to_string(), "would block");
            assert!(err.is_instance(py, family.class(py, "EAGAIN").unwrap()));
        });
    }

    #[test]
    fn an_unlisted_errno_arrives_as_the_base_class_rather_than_a_panic() {
        Python::initialize();
        Python::attach(|py| {
            let family = family(py);
            let err = family.error(py, &Errno::new("EWHAT", "a variant nobody listed"));
            assert!(err.is_instance(py, family.base(py)));
            assert_eq!(
                err.value(py)
                    .getattr("errno")
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "EWHAT"
            );
        });
    }

    #[test]
    fn a_duplicate_name_is_refused_at_import() {
        Python::initialize();
        Python::attach(|py| {
            let built = ErrorFamily::build(py, "demo", "DemoError", &["EAGAIN", "EAGAIN"]);
            assert!(built.is_err(), "a duplicated errno name must be refused");
        });
    }
}
