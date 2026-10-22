//! `http`/`https` URLs against arbitrary strings.
//!
//! Properties: parsing never panics; an accepted URL has a path starting
//! with `/`, no fragment and a non-zero port, and the URL rebuilt from its
//! parts parses to the same value.

#![no_main]

use libfuzzer_sys::fuzz_target;
use weida_http_codec::uri::Target;

fuzz_target!(|data: &[u8]| {
    let Ok(text) = core::str::from_utf8(data) else {
        return;
    };
    let Ok(t) = Target::parse(text) else {
        return;
    };
    assert!(t.path_and_query.starts_with('/'));
    assert!(!t.path_and_query.contains('#'));
    assert_ne!(t.port, 0);
    let rebuilt = format!(
        "{}://{}{}",
        t.scheme.as_str(),
        t.authority(),
        t.path_and_query
    );
    assert_eq!(Target::parse(&rebuilt).expect("a rebuilt URL parses"), t);
});
