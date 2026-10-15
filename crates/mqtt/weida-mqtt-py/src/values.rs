//! What crosses the boundary: a message out, a delivery in, and the three
//! things that describe a subscription.
//!
//! # Why a delivery is a class and not a dict
//!
//! A dict would need a key per property and a caller would have to remember
//! which keys exist. A class has attributes, so `delivery.subscription_ids`
//! either exists or raises `AttributeError` at the point of the typo rather
//! than returning `None` forever. It is also what makes the asymmetry visible:
//! a [`PyMessage`] has the fields a *publisher* sets and a [`PyDelivery`] has
//! the fields a *subscriber* reads, and the two lists differ — a delivery has
//! `dup` and `packet_id`, which nothing publishes, and a message has `retain`
//! as an instruction where a delivery has it as an observation.
//!
//! # The payload
//!
//! `bytes` in both directions, through `weida_py_core::bytes`, with the one
//! copy each direction forces happening where that module says it does. A
//! `str` is not accepted: MQTT payloads are opaque bytes and the `Payload
//! Format Indicator` is a *hint the publisher sets*, not an encoding the
//! protocol applies (3.3.2.3.2), so a binding that encoded a `str` silently
//! would be inventing one.

use std::time::Duration;

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
use weida_mqtt::{
    Completion, Delivery, Message, PayloadFormat, QoS, RetainHandling, RetainedOrigin, Subscription,
};
use weida_py_core::{payload_of, py_bytes};

/// Turns Python's QoS integer into the library's, refusing anything else at
/// the call site rather than rounding it.
pub fn qos_of(value: u8) -> PyResult<QoS> {
    match value {
        0 => Ok(QoS::AtMostOnce),
        1 => Ok(QoS::AtLeastOnce),
        2 => Ok(QoS::ExactlyOnce),
        other => Err(pyo3::exceptions::PyValueError::new_err(format!(
            "QoS is 0, 1 or 2; {other} is not one ([MQTT-3.3.1-4])"
        ))),
    }
}

/// The integer Python sees for a QoS.
pub fn qos_byte(qos: QoS) -> u8 {
    match qos {
        QoS::AtMostOnce => 0,
        QoS::AtLeastOnce => 1,
        QoS::ExactlyOnce => 2,
    }
}

/// Seconds as a float, refusing a negative one.
fn seconds(name: &str, value: Option<f64>) -> PyResult<Option<Duration>> {
    match value {
        None => Ok(None),
        Some(value) if value.is_finite() && value >= 0.0 => {
            Ok(Some(Duration::from_secs_f64(value)))
        }
        Some(value) => Err(pyo3::exceptions::PyValueError::new_err(format!(
            "{name} must be a non-negative number of seconds; {value} is not"
        ))),
    }
}

/// `weida_mqtt.Message`: one Application Message to publish.
#[pyclass(frozen, name = "Message", module = "weida_mqtt")]
pub struct PyMessage {
    pub(crate) inner: Message,
}

impl PyMessage {
    /// The library's message, as the blocking surface takes it.
    pub fn inner(&self) -> &Message {
        &self.inner
    }
}

#[pymethods]
impl PyMessage {
    /// A message on `topic` carrying `payload`.
    ///
    /// `topic` is a Topic **Name** and never a filter: `+` and `#` are refused
    /// when the message is published, because a name is not a pattern
    /// ([MQTT-3.3.2-2]).
    #[new]
    #[pyo3(signature = (
        topic,
        payload = None,
        *,
        qos = 0,
        retain = false,
        content_type = None,
        response_topic = None,
        correlation_data = None,
        message_expiry = None,
        payload_format_utf8 = false,
        user_properties = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        topic: String,
        payload: Option<&Bound<'_, PyAny>>,
        qos: u8,
        retain: bool,
        content_type: Option<String>,
        response_topic: Option<String>,
        correlation_data: Option<&Bound<'_, PyAny>>,
        message_expiry: Option<f64>,
        payload_format_utf8: bool,
        user_properties: Option<Vec<(String, String)>>,
    ) -> PyResult<PyMessage> {
        let payload = match payload {
            Some(value) => payload_of(value)?,
            None => Vec::new(),
        };
        let correlation_data = match correlation_data {
            Some(value) => Some(payload_of(value)?),
            None => None,
        };
        Ok(PyMessage {
            inner: Message {
                topic,
                payload,
                qos: qos_of(qos)?,
                retain,
                payload_format_indicator: payload_format_utf8.then_some(PayloadFormat::Utf8),
                message_expiry: seconds("message_expiry", message_expiry)?,
                content_type,
                response_topic,
                correlation_data,
                user_properties: user_properties.unwrap_or_default(),
            },
        })
    }

    /// The delete of a topic's retained message: RETAIN 1 and an empty
    /// payload.
    ///
    /// "A PUBLISH with RETAIN 1 and a zero-byte payload removes the retained
    /// message for that topic, is delivered to current subscribers as a normal
    /// message, and is itself **not stored**" ([MQTT-3.3.1-6], 3.3.1.3). It is
    /// a constructor rather than a recipe because `Message(topic, b"",
    /// retain=True)` reads like an oversight and this reads like the delete it
    /// is.
    #[staticmethod]
    #[pyo3(signature = (topic, *, qos = 0))]
    fn delete_retained(topic: String, qos: u8) -> PyResult<PyMessage> {
        Ok(PyMessage {
            inner: Message {
                qos: qos_of(qos)?,
                ..Message::delete_retained(topic)
            },
        })
    }

    /// The Topic Name.
    #[getter]
    fn topic(&self) -> &str {
        &self.inner.topic
    }

    /// The payload.
    #[getter]
    fn payload<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        py_bytes(py, &self.inner.payload)
    }

    /// 0, 1 or 2.
    #[getter]
    fn qos(&self) -> u8 {
        qos_byte(self.inner.qos)
    }

    /// Whether the server is asked to store this as the topic's retained
    /// message.
    #[getter]
    fn retain(&self) -> bool {
        self.inner.retain
    }

    fn __repr__(&self) -> String {
        format!(
            "Message(topic={:?}, {} bytes, qos={}, retain={})",
            self.inner.topic,
            self.inner.payload.len(),
            qos_byte(self.inner.qos),
            self.inner.retain
        )
    }
}

/// `weida_mqtt.Delivery`: one Application Message the server delivered.
#[pyclass(frozen, name = "Delivery", module = "weida_mqtt")]
pub struct PyDelivery {
    inner: Delivery,
}

impl PyDelivery {
    /// Wraps a delivery from the library.
    pub fn of(inner: Delivery) -> PyDelivery {
        PyDelivery { inner }
    }
}

#[pymethods]
impl PyDelivery {
    /// The Topic Name, always — a Topic Alias is resolved before a delivery
    /// reaches here, so this is never empty and never a number.
    #[getter]
    fn topic(&self) -> &str {
        &self.inner.topic
    }

    /// The payload.
    #[getter]
    fn payload<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        py_bytes(py, &self.inner.payload)
    }

    /// The QoS it was **delivered** at, which need not be the QoS it was
    /// published at: the rule is the minimum of the publish's and the
    /// subscription's granted maximum ([MQTT-3.8.4-8]).
    #[getter]
    fn qos(&self) -> u8 {
        qos_byte(self.inner.qos)
    }

    /// The RETAIN flag as it arrived. What it *means* depends on the
    /// subscription's Retain As Published — see [`PyDelivery::origin`].
    #[getter]
    fn retain(&self) -> bool {
        self.inner.retain
    }

    /// DUP. Reported because it is on the wire, not because it means
    /// anything: it is not propagated, a receiver of 1 "cannot assume that it
    /// has seen an earlier copy", and the same message can arrive twice with
    /// DUP 0 under different identifiers.
    #[getter]
    fn dup(&self) -> bool {
        self.inner.dup
    }

    /// The Packet Identifier, present only at QoS 1 and 2 (3.3.2.2).
    #[getter]
    fn packet_id(&self) -> Option<u16> {
        self.inner.packet_id
    }

    /// The Subscription Identifiers this delivery was caused by
    /// ([MQTT-3.3.4-4]).
    ///
    /// More than one is legal: overlapping subscriptions of one client may
    /// each contribute. Empty where the server sent none, which is what a
    /// server declaring `Subscription Identifiers Available` 0 does — and
    /// against such a server the answer is to match the topic against the
    /// filters this client subscribed with.
    #[getter]
    fn subscription_ids(&self) -> Vec<u32> {
        self.inner.properties.subscription_identifiers.clone()
    }

    /// Where this delivery came from, as far as the protocol can say:
    /// `"retained"`, `"live"`, or `"unknowable"`.
    ///
    /// `retain_as_published` is the option of the subscription that matched.
    /// Under 0 — the default — the server sets RETAIN only on a message sent
    /// because a subscription was made ([MQTT-3.3.1-8]) and clears it on every
    /// forwarded live message ([MQTT-3.3.1-12]), so the flag *is* the answer.
    /// Under 1 the flag is the publisher's ([MQTT-3.3.1-13]) and a cached copy
    /// is indistinguishable from a live one — which is a loss of the protocol
    /// and is named rather than guessed at.
    #[pyo3(signature = (retain_as_published = false))]
    fn origin(&self, retain_as_published: bool) -> &'static str {
        match self.inner.origin(retain_as_published) {
            RetainedOrigin::Retained => "retained",
            RetainedOrigin::Live => "live",
            RetainedOrigin::Unknowable => "unknowable",
        }
    }

    /// Every property the PUBLISH carried, as a dict.
    ///
    /// A dict here and attributes above, on purpose: the six fields above are
    /// on every delivery and a caller reads them by name, while the properties
    /// are optional and a caller most often forwards the lot.
    #[getter]
    fn properties<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let properties = &self.inner.properties;
        let dict = PyDict::new(py);
        dict.set_item(
            "payload_format_utf8",
            properties.payload_format_indicator == Some(PayloadFormat::Utf8),
        )?;
        dict.set_item("message_expiry", properties.message_expiry_interval)?;
        dict.set_item("content_type", properties.content_type.as_deref())?;
        dict.set_item("response_topic", properties.response_topic.as_deref())?;
        dict.set_item(
            "correlation_data",
            properties
                .correlation_data
                .as_deref()
                .map(|bytes| py_bytes(py, bytes)),
        )?;
        // A list of pairs and not a dict: "the same name may appear more than
        // once" ([MQTT-3.3.2-17] preserves order and repeats), and a dict
        // would silently drop every repeat but the last.
        dict.set_item("user_properties", properties.user_properties.clone())?;
        dict.set_item(
            "subscription_ids",
            properties.subscription_identifiers.clone(),
        )?;
        // Whether the two lists above are all of what the PUBLISH carried:
        // past the library's per-delivery ceilings the first are kept and
        // this says so.
        dict.set_item("truncated", properties.truncated)?;
        Ok(dict)
    }

    fn __repr__(&self) -> String {
        format!(
            "Delivery(topic={:?}, {} bytes, qos={}, retain={}, subscription_ids={:?})",
            self.inner.topic,
            self.inner.payload.len(),
            qos_byte(self.inner.qos),
            self.inner.retain,
            self.inner.properties.subscription_identifiers,
        )
    }
}

/// `weida_mqtt.Subscription`: one Topic Filter and its options.
#[pyclass(frozen, name = "Subscription", module = "weida_mqtt")]
pub struct PySubscription {
    pub(crate) inner: Subscription,
}

impl PySubscription {
    /// The library's subscription, as the blocking surface takes it.
    pub fn inner(&self) -> &Subscription {
        &self.inner
    }
}

#[pymethods]
impl PySubscription {
    /// A subscription to `filter` at `qos`, with the four options of 3.8.3.1.
    ///
    /// `retain_handling` is 0 (send retained messages at subscribe), 1 (only
    /// if the subscription did not already exist) or 2 (never). **3 is not a
    /// value**: "it is a Protocol Error to send a Retain Handling of 3"
    /// (3.8.3.1), so it is refused here rather than on the wire.
    #[new]
    #[pyo3(signature = (
        filter,
        qos = 0,
        *,
        no_local = false,
        retain_as_published = false,
        retain_handling = 0,
    ))]
    fn new(
        filter: String,
        qos: u8,
        no_local: bool,
        retain_as_published: bool,
        retain_handling: u8,
    ) -> PyResult<PySubscription> {
        let handling = match retain_handling {
            0 => RetainHandling::SendAtSubscribe,
            1 => RetainHandling::SendIfNew,
            2 => RetainHandling::DoNotSend,
            3 => {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "Retain Handling 3 is a Protocol Error to send (3.8.3.1); \
                     the values are 0, 1 and 2",
                ));
            }
            other => {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "Retain Handling is 0, 1 or 2; {other} is not one (3.8.3.1)"
                )));
            }
        };
        let mut inner = Subscription::new(filter, qos_of(qos)?);
        inner.options.no_local = no_local;
        inner.options.retain_as_published = retain_as_published;
        inner.options.retain_handling = handling;
        Ok(PySubscription { inner })
    }

    /// The Topic Filter, which may be `$share/{ShareName}/{filter}`.
    #[getter]
    fn filter(&self) -> &str {
        &self.inner.filter
    }

    /// The maximum QoS asked for. What was *granted* is the SUBACK's code.
    #[getter]
    fn qos(&self) -> u8 {
        qos_byte(self.inner.options.maximum_qos)
    }

    /// Whether this is a Shared Subscription.
    #[getter]
    fn shared(&self) -> bool {
        self.inner.is_shared()
    }

    fn __repr__(&self) -> String {
        format!(
            "Subscription(filter={:?}, qos={}, no_local={}, retain_as_published={}, \
             retain_handling={})",
            self.inner.filter,
            qos_byte(self.inner.options.maximum_qos),
            self.inner.options.no_local,
            self.inner.options.retain_as_published,
            self.inner.options.retain_handling as u8,
        )
    }
}

/// `weida_mqtt.Completion`: what a publish's hop certified, and nothing more.
///
/// The three kinds are not interchangeable and the type says so, because
/// "published" means three different things at the three QoS levels and none
/// of them means durability or delivery to a subscriber.
#[pyclass(frozen, name = "Completion", module = "weida_mqtt")]
pub struct PyCompletion {
    kind: &'static str,
    reason_code: Option<u8>,
}

impl PyCompletion {
    /// Wraps a completion from the library.
    pub fn of(completion: Completion) -> PyCompletion {
        match completion {
            // QoS 0: the bytes left this process. Nothing else is claimed,
            // and nothing else can be (4.3.1).
            Completion::Sent => PyCompletion {
                kind: "sent",
                reason_code: None,
            },
            Completion::Acknowledged(code) => PyCompletion {
                kind: "acknowledged",
                reason_code: Some(code.as_byte()),
            },
            Completion::Complete(code) => PyCompletion {
                kind: "complete",
                reason_code: Some(code.as_byte()),
            },
            Completion::Refused(code) => PyCompletion {
                kind: "refused",
                reason_code: Some(code.as_byte()),
            },
        }
    }
}

#[pymethods]
impl PyCompletion {
    /// `"sent"` at QoS 0, `"acknowledged"` on a PUBACK, `"complete"` on a
    /// PUBCOMP, `"refused"` on a PUBREC of 0x80 or above.
    #[getter]
    fn kind(&self) -> &'static str {
        self.kind
    }

    /// The acknowledgement's reason code, or `None` at QoS 0 where there is no
    /// acknowledgement at all.
    #[getter]
    fn reason_code(&self) -> Option<u8> {
        self.reason_code
    }

    /// Whether the hop accepted the message.
    ///
    /// A `"refused"` completion is **not** a failure of the call: the exchange
    /// ended the way the protocol says it should and the message "MUST NOT be
    /// retransmitted" ([MQTT-4.4.0-2]), so it is returned rather than raised
    /// and this is how a caller asks.
    #[getter]
    fn accepted(&self) -> bool {
        self.kind != "refused"
    }

    fn __repr__(&self) -> String {
        match self.reason_code {
            Some(code) => format!("Completion({}, 0x{code:02X})", self.kind),
            None => format!("Completion({})", self.kind),
        }
    }
}
