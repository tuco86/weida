//! The whole packet decoder against arbitrary bytes, under a small cap.
//!
//! Property: decoding never panics, an accepted packet fits the cap, it
//! reports its own encoded length as the bytes the *encoder* will write, and
//! re-encoding yields something that decodes to the same value.
//!
//! Re-decoding rather than re-encoding is deliberate, and it is the difference
//! between fuzzing a codec and fuzzing a serializer. A peer may send
//! properties in any order and may spell an acknowledgement's all-success tail
//! in two, three or four bytes (3.4.1) [mqtt5 §3]; this codec emits one
//! canonical form. Demanding byte equality with the input would therefore
//! report legal inputs as failures, while demanding *value* equality catches
//! the faults that matter: a field silently dropped, a repeatable property
//! lost by the walker, a payload-list entry skipped, or a length computed
//! differently by `body_len` than by the encoder.
//!
//! The one length relation that holds is `encoded_len <= used + 1`. The
//! canonical spelling is never longer than the one that arrived, except by the
//! byte a reason code sent *without* its Property Length leaves out: an
//! acknowledgement's `id, code` body (3.4.2.2), DISCONNECT's bare code
//! (3.14.2.1) and AUTH's bare code all decode, and all re-encode with the
//! explicit zero length that every peer writes, one byte longer. Never longer
//! than that, and always a fixed point: decoding the canonical form and
//! encoding it again gives the same bytes.
//!
//! The cap stays small so that the allocation bound is exercised rather than
//! described: a declared quarter-gigabyte body over a fifty-byte input must be
//! refused from the fixed header, and a decoder that reserved first would be
//! killed by the memory limit instead of caught here.

#![no_main]

use libfuzzer_sys::fuzz_target;
use weida_mqtt_codec::Packet;

const CAP: u32 = 4096;

fuzz_target!(|data: &[u8]| {
    if let Ok((packet, used)) = Packet::decode(data, CAP) {
        assert!(used <= data.len());
        assert!(used as u32 <= CAP);

        let mut out = Vec::new();
        packet.encode(&mut out).expect("a decoded packet re-encodes");
        let size = packet.encoded_len().expect("a decoded packet has a length");
        assert_eq!(size as usize, out.len(), "encoded_len agrees with encode");
        assert!(
            out.len() <= used + 1,
            "the canonical form is at most the omitted Property Length longer than what arrived"
        );

        let (again, used2) = Packet::decode(&out, CAP).expect("the canonical form decodes");
        assert_eq!(again, packet);
        assert_eq!(used2, out.len());
        assert_eq!(again.packet_type(), packet.packet_type());
        let mut twice = Vec::new();
        again.encode(&mut twice).expect("the canonical form re-encodes");
        assert_eq!(twice, out, "the canonical form is a fixed point");

        // The refusal on the way out is the mirror of the one on the way in.
        let mut small = Vec::new();
        assert!(packet.encode_within(size - 1, &mut small).is_err());
        assert!(small.is_empty(), "a refused encode writes nothing");
    }
});
