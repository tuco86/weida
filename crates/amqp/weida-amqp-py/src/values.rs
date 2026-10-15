//! The wire's values as Python objects, and back.
//!
//! Four conversions and no protocol decisions:
//!
//! * a **delivery** becomes [`PyDelivery`], carrying its ids, its settled
//!   flag, its payload and — decoded on demand — its sections;
//! * an **outcome** becomes [`PyOutcome`], which answers the two questions an
//!   application has: will the message come back, and does this attempt count;
//! * a **message** is built from Python keyword arguments into the codec's
//!   [`Message`](weida_amqp_codec::message::Message);
//! * the two **settle modes** and the two **roles** are spelled as the
//!   specification spells them, and a name outside the set is refused rather
//!   than defaulted.
//!
//! # Sections are decoded on demand, not on arrival
//!
//! A delivery arrives as octets and `payload` hands them over unchanged. The
//! annotations, properties and application-properties are decoded only when
//! Python asks, because a forwarder that never looks should not pay for the
//! decode and because the octets are the message's identity — Part 3's bare
//! message "MUST NOT be modified", so the octets are what a signature is
//! computed over.

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};
use weida_amqp::settlement::Outcome;
use weida_amqp::{Condition, Delivery};
use weida_amqp_codec::message::{Body, Message, MessageId, Properties};
use weida_amqp_codec::{ReceiverSettleMode, SenderSettleMode, Value};
use weida_py_core::py_bytes;

use crate::errors;

/// `weida_amqp.Delivery`: one message, however many frames it took.
#[pyclass(frozen, name = "Delivery", module = "weida_amqp")]
pub struct PyDelivery {
    inner: Delivery,
}

impl PyDelivery {
    pub fn of(inner: Delivery) -> PyDelivery {
        PyDelivery { inner }
    }
}

#[pymethods]
impl PyDelivery {
    /// The session-scoped id a `disposition` names, and the number every
    /// settle call here takes.
    #[getter]
    fn delivery_id(&self) -> u32 {
        self.inner.delivery_id
    }

    /// The sender's tag. What identifies a delivery across a re-attach, which
    /// is why it is here beside the id.
    #[getter]
    fn delivery_tag<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        py_bytes(py, &self.inner.delivery_tag)
    }

    /// Part 3 defines exactly one format and numbers it zero.
    #[getter]
    fn message_format(&self) -> u32 {
        self.inner.message_format
    }

    /// Whether the **sender** settled as it sent. `True` means no disposition
    /// is expected, wanted or answerable: the sender has already forgotten it,
    /// so settling this delivery writes nothing.
    #[getter]
    fn settled(&self) -> bool {
        self.inner.settled
    }

    /// Our own handle for the link it arrived on.
    #[getter]
    fn handle(&self) -> u32 {
        self.inner.handle
    }

    /// The message's octets, every section, exactly as they arrived.
    #[getter]
    fn payload<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        py_bytes(py, self.inner.payload())
    }

    /// The body, decoded: `bytes` for one or more `data` sections, the value
    /// for an `amqp-value`, a list for an `amqp-sequence`, and `None` for a
    /// message with no body section at all — which Part 3 does not permit and
    /// a peer may still send.
    fn body<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
        let message = self
            .inner
            .message()
            .map_err(|error| errors::configuration(py, error.to_string()))?;
        Ok(match &message.body {
            Body::Empty => None,
            Body::Data(sections) => {
                // One section, the common case, is copied once, straight into
                // the `bytes`. Several are joined in place, so no intermediate
                // buffer holds the whole body a second time.
                let bytes = match sections.as_slice() {
                    [only] => py_bytes(py, only),
                    many => {
                        let total: usize = many.iter().map(|section| section.len()).sum();
                        PyBytes::new_with(py, total, |buffer| {
                            let mut at = 0;
                            for section in many {
                                buffer[at..at + section.len()].copy_from_slice(section);
                                at += section.len();
                            }
                            Ok(())
                        })?
                    }
                };
                Some(bytes.into_any())
            }
            Body::Value(value) => Some(value_to_py(py, value)?),
            // One entry per `amqp-sequence` section, each of which is itself
            // a list; flattening them would lose the section boundaries the
            // sender chose.
            Body::Sequence(sections) => {
                let list = PyList::empty(py);
                for section in sections {
                    list.append(value_to_py(py, section)?)?;
                }
                Some(list.into_any())
            }
        })
    }

    /// `properties`, as a dict of the thirteen fields that were present.
    fn properties<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyDict>>> {
        let message = self
            .inner
            .message()
            .map_err(|error| errors::configuration(py, error.to_string()))?;
        message
            .properties
            .as_ref()
            .map(|properties| properties_to_py(py, properties))
            .transpose()
    }

    /// `application-properties`, as a dict. The section Part 3 restricts to
    /// simple values, which is what makes it the one an intermediary filters
    /// on.
    fn application_properties<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyDict>>> {
        self.map_section(py, |message| message.application_properties.clone())
    }

    /// `message-annotations`: what travels with the message indefinitely.
    fn message_annotations<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyDict>>> {
        self.map_section(py, |message| message.message_annotations.clone())
    }

    /// `delivery-annotations`: one hop only, sender to receiver.
    fn delivery_annotations<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyDict>>> {
        self.map_section(py, |message| message.delivery_annotations.clone())
    }

    /// `footer`: the values computable only once the whole bare message is
    /// seen.
    fn footer<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyDict>>> {
        self.map_section(py, |message| message.footer.clone())
    }

    /// `header`, as a dict of the five fields.
    fn header<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyDict>>> {
        let message = self
            .inner
            .message()
            .map_err(|error| errors::configuration(py, error.to_string()))?;
        let Some(header) = message.header else {
            return Ok(None);
        };
        let dict = PyDict::new(py);
        dict.set_item("durable", header.durable)?;
        dict.set_item("priority", header.priority)?;
        dict.set_item("ttl", header.ttl)?;
        dict.set_item("first_acquirer", header.first_acquirer)?;
        dict.set_item("delivery_count", header.delivery_count)?;
        Ok(Some(dict))
    }

    fn __repr__(&self) -> String {
        format!(
            "<Delivery id={} tag={:?} settled={} {} octets>",
            self.inner.delivery_id,
            self.inner.delivery_tag,
            self.inner.settled,
            self.inner.len()
        )
    }
}

impl PyDelivery {
    /// One map-valued section, decoded on demand.
    fn map_section<'py, F>(&self, py: Python<'py>, pick: F) -> PyResult<Option<Bound<'py, PyDict>>>
    where
        F: for<'a> FnOnce(&Message<'a>) -> Option<Value<'a>>,
    {
        let message = self
            .inner
            .message()
            .map_err(|error| errors::configuration(py, error.to_string()))?;
        let Some(value) = pick(&message) else {
            return Ok(None);
        };
        Ok(Some(map_to_py(py, &value)?))
    }
}

/// `weida_amqp.Outcome`: what the peer committed to.
#[pyclass(frozen, name = "Outcome", module = "weida_amqp")]
pub struct PyOutcome {
    inner: Outcome,
}

impl PyOutcome {
    pub fn of(inner: Outcome) -> PyOutcome {
        PyOutcome { inner }
    }
}

#[pymethods]
impl PyOutcome {
    /// `accepted`, `rejected`, `released` or `modified`, as the specification
    /// spells them.
    #[getter]
    fn name(&self) -> &'static str {
        self.inner.name()
    }

    /// Whether the message may reach a consumer again. The question an
    /// application asked the outcome for: `accepted` and `rejected` end the
    /// message's life at this node, `released` and `modified` put it back.
    #[getter]
    fn may_be_redelivered(&self) -> bool {
        self.inner.may_be_redelivered()
    }

    /// Whether this attempt counts against a redelivery limit.
    #[getter]
    fn increments_delivery_count(&self) -> bool {
        self.inner.increments_delivery_count()
    }

    /// The condition a `rejected` carried, as `(condition, description)`, or
    /// `None`. A broker that rejects without a reason leaves an application
    /// with nothing to log, which is why this is not flattened away.
    #[getter]
    fn error(&self) -> Option<(String, Option<String>)> {
        match &self.inner {
            Outcome::Rejected { error } => error
                .as_ref()
                .map(|error| (error.condition.clone(), error.description.clone())),
            _ => None,
        }
    }

    /// `modified.delivery-failed`, or `None` where the peer left it unset —
    /// and unset is not `False`: it leaves the node's own policy in charge.
    #[getter]
    fn delivery_failed(&self) -> Option<bool> {
        match &self.inner {
            Outcome::Modified {
                delivery_failed, ..
            } => *delivery_failed,
            _ => None,
        }
    }

    /// `modified.undeliverable-here`, or `None` where unset.
    #[getter]
    fn undeliverable_here(&self) -> Option<bool> {
        match &self.inner {
            Outcome::Modified {
                undeliverable_here, ..
            } => *undeliverable_here,
            _ => None,
        }
    }

    fn __repr__(&self) -> String {
        format!("<Outcome {}>", self.inner.name())
    }

    /// So that `outcome == "accepted"` works, which is what a test wants to
    /// write and what a reader expects to see.
    fn __eq__(&self, other: &Bound<'_, PyAny>) -> bool {
        other
            .extract::<String>()
            .is_ok_and(|name| name == self.inner.name())
    }
}

/// Builds the outcome a Python name asks for.
///
/// The four terminal states and nothing else: `received` is not an outcome,
/// and a name outside the set is refused rather than defaulted, because
/// defaulting a misspelled "acccepted" to `accepted` would settle a delivery
/// the caller meant to reject.
pub fn outcome_of(
    py: Python<'_>,
    name: &str,
    error: Option<(String, Option<String>)>,
    delivery_failed: Option<bool>,
    undeliverable_here: Option<bool>,
) -> PyResult<Outcome> {
    match name {
        "accepted" => Ok(Outcome::Accepted),
        "released" => Ok(Outcome::Released),
        "rejected" => Ok(Outcome::Rejected {
            error: error.map(|(condition, description)| match description {
                Some(description) => Condition::described(condition, description),
                None => Condition::new(condition),
            }),
        }),
        "modified" => Ok(Outcome::Modified {
            delivery_failed,
            undeliverable_here,
            message_annotations: None,
        }),
        "received" => Err(errors::configuration(
            py,
            "`received` is not an outcome: it is the one non-terminal delivery \
             state and describes how far a partial body got, so settling with \
             it would be settling on a progress report",
        )),
        other => Err(errors::configuration(
            py,
            format!(
                "{other:?} is not a delivery outcome; Part 3 §3.4 names four: \
                 accepted, rejected, released, modified"
            ),
        )),
    }
}

/// The sender settle mode a Python name asks for.
pub fn sender_settle_mode(py: Python<'_>, name: &str) -> PyResult<SenderSettleMode> {
    match name {
        "unsettled" => Ok(SenderSettleMode::Unsettled),
        "settled" => Ok(SenderSettleMode::Settled),
        "mixed" => Ok(SenderSettleMode::Mixed),
        other => Err(errors::configuration(
            py,
            format!(
                "{other:?} is not a snd-settle-mode; Part 2 §2.7.3 names three: \
                 unsettled, settled, mixed"
            ),
        )),
    }
}

/// The receiver settle mode a Python name asks for.
pub fn receiver_settle_mode(py: Python<'_>, name: &str) -> PyResult<ReceiverSettleMode> {
    match name {
        "first" => Ok(ReceiverSettleMode::First),
        "second" => Ok(ReceiverSettleMode::Second),
        other => Err(errors::configuration(
            py,
            format!(
                "{other:?} is not a rcv-settle-mode; Part 2 §2.7.3 names two: \
                 first, second"
            ),
        )),
    }
}

/// The name of a settle mode, for reporting what the peer actually chose.
pub const fn sender_mode_name(mode: SenderSettleMode) -> &'static str {
    match mode {
        SenderSettleMode::Unsettled => "unsettled",
        SenderSettleMode::Settled => "settled",
        SenderSettleMode::Mixed => "mixed",
    }
}

/// The name of a receiver settle mode.
pub const fn receiver_mode_name(mode: ReceiverSettleMode) -> &'static str {
    match mode {
        ReceiverSettleMode::First => "first",
        ReceiverSettleMode::Second => "second",
    }
}

/// What Python passed for one message, before it becomes sections.
///
/// A struct rather than eight arguments, because five of them are
/// `properties` fields and one is a `header` field, and a call site that
/// mixed the two up would build a message whose immutable half said something
/// the caller did not mean.
#[derive(Default)]
pub struct Outgoing<'a> {
    /// `bytes` becomes a `data` section, which is what a payload is.
    pub body: &'a [u8],
    /// A `str` becomes an `amqp-value` of a string, which is what every
    /// broker's own example sends.
    pub text: bool,
    /// `properties.subject`.
    pub subject: Option<&'a str>,
    /// `properties.message-id`, as a string. The field allows four types and
    /// this surface offers one: a caller that needs a ulong or a uuid builds
    /// the sections itself, because guessing a Python object's AMQP type
    /// would be guessing about the wire.
    pub message_id: Option<&'a str>,
    /// `properties.correlation-id`.
    pub correlation_id: Option<&'a str>,
    /// `properties.content-type`, which SHOULD NOT be set for a body that is
    /// not a `data` section.
    pub content_type: Option<&'a str>,
    /// `properties.reply-to`.
    pub reply_to: Option<&'a str>,
    /// `header.durable`: a **demand**, not a hint. A target that cannot
    /// honour it MUST refuse the message rather than accept it, which is why
    /// it is here and not among the properties.
    pub durable: bool,
}

/// Builds a message from what Python passed.
pub fn message_from<'a>(py: Python<'_>, outgoing: &Outgoing<'a>) -> PyResult<Message<'a>> {
    let body = if outgoing.text {
        let text = std::str::from_utf8(outgoing.body)
            .map_err(|error| errors::configuration(py, format!("body is not UTF-8: {error}")))?;
        Body::Value(Value::String(text))
    } else {
        Body::Data(vec![outgoing.body])
    };
    let has_properties = outgoing.subject.is_some()
        || outgoing.message_id.is_some()
        || outgoing.correlation_id.is_some()
        || outgoing.content_type.is_some()
        || outgoing.reply_to.is_some();
    Ok(Message {
        header: outgoing.durable.then(|| weida_amqp_codec::message::Header {
            durable: true,
            ..weida_amqp_codec::message::Header::default()
        }),
        properties: has_properties.then(|| Properties {
            message_id: outgoing.message_id.map(MessageId::String),
            correlation_id: outgoing.correlation_id.map(MessageId::String),
            subject: outgoing.subject,
            content_type: outgoing.content_type,
            reply_to: outgoing.reply_to,
            ..Properties::default()
        }),
        body,
        ..Message::default()
    })
}

/// One AMQP value as the nearest Python object.
///
/// Only the shapes a message body or a properties map can hold. A value this
/// function does not know becomes its `Debug` text rather than an error: a
/// delivery a caller cannot read at all is worse than one field it has to
/// squint at, and the octets are always there in `payload`.
fn value_to_py<'py>(py: Python<'py>, value: &Value<'_>) -> PyResult<Bound<'py, PyAny>> {
    Ok(match value {
        Value::Null => py.None().into_bound(py),
        Value::Boolean(flag) => flag.into_pyobject(py)?.to_owned().into_any(),
        Value::Ubyte(n) => n.into_pyobject(py)?.into_any(),
        Value::Ushort(n) => n.into_pyobject(py)?.into_any(),
        Value::Uint(n) => n.into_pyobject(py)?.into_any(),
        Value::Ulong(n) => n.into_pyobject(py)?.into_any(),
        Value::Byte(n) => n.into_pyobject(py)?.into_any(),
        Value::Short(n) => n.into_pyobject(py)?.into_any(),
        Value::Int(n) => n.into_pyobject(py)?.into_any(),
        Value::Long(n) | Value::Timestamp(n) => n.into_pyobject(py)?.into_any(),
        Value::Float(n) => n.into_pyobject(py)?.into_any(),
        Value::Double(n) => n.into_pyobject(py)?.into_any(),
        Value::String(text) | Value::Symbol(text) => text.into_pyobject(py)?.into_any(),
        Value::Binary(octets) => py_bytes(py, octets).into_any(),
        Value::Uuid(bytes) => py_bytes(py, bytes).into_any(),
        Value::List(items) => {
            let list = PyList::empty(py);
            for item in items {
                list.append(value_to_py(py, item)?)?;
            }
            list.into_any()
        }
        // An array's elements all share one constructor, which is a wire
        // detail with no Python counterpart: a list is what a caller wants.
        Value::Array(array) => {
            let list = PyList::empty(py);
            for item in array.items() {
                list.append(value_to_py(py, item)?)?;
            }
            list.into_any()
        }
        Value::Map(_) => map_to_py(py, value)?.into_any(),
        other => format!("{other:?}").into_pyobject(py)?.into_any(),
    })
}

/// A `map` value as a dict. Anything else is a dict of one entry keyed
/// `"value"`, so that a caller never gets `None` where a section existed.
fn map_to_py<'py>(py: Python<'py>, value: &Value<'_>) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    match value {
        Value::Map(entries) => {
            for (key, value) in entries {
                dict.set_item(value_to_py(py, key)?, value_to_py(py, value)?)?;
            }
        }
        other => {
            dict.set_item("value", value_to_py(py, other)?)?;
        }
    }
    Ok(dict)
}

/// The thirteen fields of `properties`, those that were set.
fn properties_to_py<'py>(
    py: Python<'py>,
    properties: &Properties<'_>,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    if let Some(id) = &properties.message_id {
        dict.set_item("message_id", message_id_to_py(py, id)?)?;
    }
    if let Some(user) = properties.user_id {
        dict.set_item("user_id", py_bytes(py, user))?;
    }
    for (name, field) in [
        ("to", properties.to),
        ("subject", properties.subject),
        ("reply_to", properties.reply_to),
        ("content_type", properties.content_type),
        ("content_encoding", properties.content_encoding),
        ("group_id", properties.group_id),
        ("reply_to_group_id", properties.reply_to_group_id),
    ] {
        if let Some(value) = field {
            dict.set_item(name, value)?;
        }
    }
    if let Some(id) = &properties.correlation_id {
        dict.set_item("correlation_id", message_id_to_py(py, id)?)?;
    }
    if let Some(when) = properties.absolute_expiry_time {
        dict.set_item("absolute_expiry_time", when)?;
    }
    if let Some(when) = properties.creation_time {
        dict.set_item("creation_time", when)?;
    }
    if let Some(sequence) = properties.group_sequence {
        dict.set_item("group_sequence", sequence)?;
    }
    Ok(dict)
}

/// A `message-id` or `correlation-id` in whichever of its four types it
/// arrived as.
fn message_id_to_py<'py>(py: Python<'py>, id: &MessageId<'_>) -> PyResult<Bound<'py, PyAny>> {
    Ok(match id {
        MessageId::Ulong(n) => n.into_pyobject(py)?.into_any(),
        MessageId::Uuid(bytes) => py_bytes(py, bytes).into_any(),
        MessageId::Binary(octets) => py_bytes(py, octets).into_any(),
        MessageId::String(text) => text.into_pyobject(py)?.into_any(),
    })
}
