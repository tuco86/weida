//! Header codec microbenchmarks.
//!
//! The header is on the hot path of every transfer, so its cost has to be known
//! rather than assumed: a 40-byte transfer pays this per message.

use criterion::{Criterion, criterion_group, criterion_main};
use std::hint::black_box;
use weida_core::ErrorCode;
use weida_protocol::{
    DataHeader, ErrorHeader, FrameKind, Hello, ReportMode, encode_frame, encode_preamble,
    parse_preamble,
};

fn minimal_request() -> DataHeader {
    DataHeader::addressed("/transform")
}

fn full_request() -> DataHeader {
    DataHeader {
        endpoint: Some("/transform".into()),
        content_len: Some(1 << 40),
        content_type: Some("application/octet-stream".into()),
        traceparent: Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".into()),
        tracestate: Some("vendor=value,other=thing".into()),
        topic: Some("px.eur".into()),
        // Keys 6 through 13 stay absent so this bench keeps measuring the
        // same header as the numbers recorded in IMPLEMENTATION.md §4.2;
        // their cost is the separate measurement of IMPLEMENTATION.md §4.4.
        sequence: None,
        producer: None,
        achieved: None,
        report_id: None,
        report: Vec::new(),
        report_mode: ReportMode::Progress,
        segment: None,
        layer: None,
    }
}

fn bench_data_header(c: &mut Criterion) {
    let minimal = minimal_request();
    let full = full_request();
    let minimal_bytes = minimal.encode();
    let full_bytes = full.encode();

    let mut group = c.benchmark_group("data_header");
    group.bench_function("encode_minimal", |b| {
        b.iter(|| black_box(black_box(&minimal).encode()))
    });
    group.bench_function("decode_minimal", |b| {
        b.iter(|| DataHeader::decode(black_box(&minimal_bytes)).expect("valid"))
    });
    group.bench_function("encode_full", |b| {
        b.iter(|| black_box(black_box(&full).encode()))
    });
    group.bench_function("decode_full", |b| {
        b.iter(|| DataHeader::decode(black_box(&full_bytes)).expect("valid"))
    });
    group.finish();
}

fn bench_control_frames(c: &mut Criterion) {
    let hello = Hello::v0(16 * 1024, 2048);
    let hello_bytes = hello.encode();
    let error = ErrorHeader::new(ErrorCode::NoReply);
    let error_bytes = error.encode();

    let mut group = c.benchmark_group("control");
    group.bench_function("hello_encode", |b| {
        b.iter(|| black_box(black_box(&hello).encode()))
    });
    group.bench_function("hello_decode", |b| {
        b.iter(|| Hello::decode(black_box(&hello_bytes)).expect("valid"))
    });
    group.bench_function("error_encode", |b| {
        b.iter(|| black_box(black_box(&error).encode()))
    });
    group.bench_function("error_decode", |b| {
        b.iter(|| ErrorHeader::decode(black_box(&error_bytes)).expect("valid"))
    });
    group.finish();
}

fn bench_framing(c: &mut Criterion) {
    let header = minimal_request().encode();
    let frame = encode_frame(FrameKind::Data, &header);

    let mut group = c.benchmark_group("framing");
    group.bench_function("encode_preamble", |b| {
        b.iter(|| {
            let mut out = Vec::with_capacity(16);
            encode_preamble(FrameKind::Data, black_box(5), &mut out);
            black_box(out)
        })
    });
    group.bench_function("parse_preamble", |b| {
        b.iter(|| parse_preamble(black_box(&frame), 16 * 1024).expect("valid"))
    });
    // The whole per-transfer serialization cost: preamble plus header.
    group.bench_function("encode_frame", |b| {
        b.iter(|| black_box(encode_frame(FrameKind::Data, black_box(&header))))
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_data_header,
    bench_control_frames,
    bench_framing
);
criterion_main!(benches);
