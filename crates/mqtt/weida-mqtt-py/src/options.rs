//! What a client puts in CONNECT, and the two ceilings the protocol leaves to
//! the caller.
//!
//! Every option is refused at construction rather than on the wire, which is
//! the rule the library follows and the reason this class exists instead of a
//! dict of keywords on `connect`: a `ValueError` at the line that builds the
//! options names the option, and a Protocol Error from the broker names a
//! byte.

use std::time::Duration;

use pyo3::prelude::*;
use weida_mqtt::{ConnectOptions, WillMessage};
use weida_py_core::payload_of;

use crate::values::{qos_of, seconds};

/// `weida_mqtt.Will`: the message the server publishes when a connection ends
/// abnormally (3.1.2.5).
///
/// It is declared in CONNECT and kept by the server, which is why it is
/// configuration rather than a call: a client cannot publish its own Will
/// except by asking for it in the DISCONNECT.
#[pyclass(frozen, name = "Will", module = "weida_mqtt")]
pub struct PyWill {
    pub(crate) inner: WillMessage,
}

#[pymethods]
impl PyWill {
    /// A Will on `topic` carrying `payload`.
    ///
    /// `delay` is the `Will Delay Interval`: the server publishes the Will
    /// after this interval or at session end, whichever is first, and MUST NOT
    /// publish it at all if a new connection to that session arrives first
    /// ([MQTT-3.1.3-9]). Absent means immediately.
    #[new]
    #[pyo3(signature = (
        topic,
        payload = None,
        *,
        qos = 0,
        retain = false,
        delay = None,
        message_expiry = None,
        content_type = None,
        response_topic = None,
        correlation_data = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        topic: String,
        payload: Option<&Bound<'_, PyAny>>,
        qos: u8,
        retain: bool,
        delay: Option<f64>,
        message_expiry: Option<f64>,
        content_type: Option<String>,
        response_topic: Option<String>,
        correlation_data: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyWill> {
        let payload = match payload {
            Some(value) => payload_of(value)?,
            None => Vec::new(),
        };
        let correlation_data = match correlation_data {
            Some(value) => Some(payload_of(value)?),
            None => None,
        };
        Ok(PyWill {
            inner: WillMessage {
                topic,
                payload,
                qos: qos_of(qos)?,
                retain,
                delay: seconds("delay", delay)?,
                message_expiry: seconds("message_expiry", message_expiry)?,
                content_type,
                response_topic,
                correlation_data,
            },
        })
    }

    /// The Will Topic.
    #[getter]
    fn topic(&self) -> &str {
        &self.inner.topic
    }

    fn __repr__(&self) -> String {
        format!(
            "Will(topic={:?}, {} bytes, qos={}, retain={})",
            self.inner.topic,
            self.inner.payload.len(),
            crate::values::qos_byte(self.inner.qos),
            self.inner.retain
        )
    }
}

/// `weida_mqtt.ConnectOptions`: everything a CONNECT declares.
#[pyclass(frozen, name = "ConnectOptions", module = "weida_mqtt")]
pub struct PyConnectOptions {
    inner: ConnectOptions,
}

impl PyConnectOptions {
    /// The options a call was given, or the defaults.
    ///
    /// A `clone` rather than a borrow because the library's options are owned
    /// and move into the connection; the alternative is a lifetime travelling
    /// into a spawned future, which cannot be done.
    pub fn resolve(options: Option<&PyConnectOptions>) -> PyResult<ConnectOptions> {
        Ok(match options {
            Some(options) => options.inner.clone(),
            None => ConnectOptions::default(),
        })
    }
}

#[pymethods]
impl PyConnectOptions {
    /// The options for `client_id`.
    ///
    /// A zero-length `client_id` asks the server to assign one and return it
    /// as `Assigned Client Identifier` ([MQTT-3.1.3-6]), readable afterwards
    /// as `Client.client_id`.
    ///
    /// `keep_alive` of 0 disables the mechanism entirely (3.1.2.10) — and
    /// with it the server's liveness detection, which is what makes it worth
    /// saying rather than defaulting to quietly.
    ///
    /// `receive_maximum` is what the **server** may have in flight toward this
    /// client, and it is a different number from what the server declares for
    /// itself: the server's is a ceiling on this client's publishes and is not
    /// configurable here because it is the server's to state.
    #[new]
    #[pyo3(signature = (
        client_id = "",
        *,
        clean_start = true,
        keep_alive = 60.0,
        session_expiry = None,
        user_name = None,
        password = None,
        will = None,
        user_properties = None,
        receive_maximum = None,
        maximum_packet_size = None,
        topic_alias_maximum = None,
        max_subscription_ids = None,
        incoming_queue = None,
        connect_timeout = None,
        ping_timeout = None,
        request_response_information = false,
        request_problem_information = true,
        authentication_method = None,
        authentication_data = None,
    ))]
    #[allow(clippy::too_many_arguments, clippy::needless_pass_by_value)]
    fn new(
        client_id: &str,
        clean_start: bool,
        keep_alive: f64,
        session_expiry: Option<f64>,
        user_name: Option<String>,
        password: Option<&Bound<'_, PyAny>>,
        will: Option<PyRef<'_, PyWill>>,
        user_properties: Option<Vec<(String, String)>>,
        receive_maximum: Option<u16>,
        maximum_packet_size: Option<u32>,
        topic_alias_maximum: Option<u16>,
        max_subscription_ids: Option<usize>,
        incoming_queue: Option<usize>,
        connect_timeout: Option<f64>,
        ping_timeout: Option<f64>,
        request_response_information: bool,
        request_problem_information: bool,
        authentication_method: Option<String>,
        authentication_data: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyConnectOptions> {
        let mut inner = ConnectOptions::new(client_id);
        inner.clean_start = clean_start;
        inner.keep_alive = seconds("keep_alive", Some(keep_alive))?.unwrap_or(Duration::ZERO);
        inner.session_expiry = seconds("session_expiry", session_expiry)?;
        inner.user_name = user_name;
        inner.password = match password {
            Some(value) => Some(payload_of(value)?),
            None => None,
        };
        inner.will = will.map(|will| will.inner.clone());
        inner.user_properties = user_properties.unwrap_or_default();
        inner.request_response_information = request_response_information;
        inner.request_problem_information = request_problem_information;
        inner.authentication_method = authentication_method;
        inner.authentication_data = match authentication_data {
            Some(value) => Some(payload_of(value)?),
            None => None,
        };
        if let Some(value) = receive_maximum {
            inner.limits.receive_maximum = value;
        }
        if let Some(value) = maximum_packet_size {
            inner.limits.maximum_packet_size = value;
        }
        if let Some(value) = topic_alias_maximum {
            inner.limits.topic_alias_maximum = value;
        }
        if let Some(value) = max_subscription_ids {
            inner.limits.max_subscription_identifiers = value;
        }
        if let Some(value) = incoming_queue {
            inner.limits.incoming_queue = value;
        }
        if let Some(value) = seconds("connect_timeout", connect_timeout)? {
            inner.connect_timeout = value;
        }
        inner.ping_timeout = seconds("ping_timeout", ping_timeout)?;

        // Refused here rather than on the wire: a `ValueError` at the line
        // that built the options names the option, and a Protocol Error from
        // the broker names a byte.
        inner
            .validate()
            .map_err(|error| pyo3::exceptions::PyValueError::new_err(error.to_string()))?;
        Ok(PyConnectOptions { inner })
    }

    /// The Client Identifier, empty where the server is asked to assign one.
    #[getter]
    fn client_id(&self) -> &str {
        &self.inner.client_id
    }

    /// Clean Start: `True` discards any existing session ([MQTT-3.1.2-4]),
    /// `False` resumes one if it exists ([MQTT-3.1.2-5]).
    #[getter]
    fn clean_start(&self) -> bool {
        self.inner.clean_start
    }

    /// The keep-alive interval in seconds; 0 means the mechanism is off.
    #[getter]
    fn keep_alive(&self) -> f64 {
        self.inner.keep_alive.as_secs_f64()
    }

    fn __repr__(&self) -> String {
        format!(
            "ConnectOptions(client_id={:?}, clean_start={}, keep_alive={})",
            self.inner.client_id,
            self.inner.clean_start,
            self.inner.keep_alive.as_secs_f64()
        )
    }
}
