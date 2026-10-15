//! Every integration test of this crate in one binary: each file below is a
//! module rather than a test target of its own, so the crate's dependencies
//! are linked once per configuration instead of once per file.

mod harness;

mod blocking;
mod connection;
mod hardening;
mod interop_mqtt5;
mod interop_rumqttd;
mod qos;
mod retain_will;
mod session;
mod shared_alias_reqrep;
mod subscriptions;
mod tls_auth;
