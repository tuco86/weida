//! The one date format a sender writes: IMF-fixdate (RFC 9110 §5.6.7).
//!
//! Days to a civil date by Howard Hinnant's `civil_from_days` (public domain,
//! <https://howardhinnant.github.io/date_algorithms.html>), inline, so that
//! the crate needs no date dependency.

const DAYS: [&[u8; 3]; 7] = [b"Sun", b"Mon", b"Tue", b"Wed", b"Thu", b"Fri", b"Sat"];
const MONTHS: [&[u8; 3]; 12] = [
    b"Jan", b"Feb", b"Mar", b"Apr", b"May", b"Jun", b"Jul", b"Aug", b"Sep", b"Oct", b"Nov", b"Dec",
];

/// Writes `unix_secs` (seconds since 1970-01-01T00:00:00Z) as an
/// IMF-fixdate, `Sun, 06 Nov 1994 08:49:37 GMT`: 29 octets.
pub fn imf_fixdate(unix_secs: u64, out: &mut Vec<u8>) {
    let days = unix_secs / 86_400;
    let secs = unix_secs % 86_400;
    // 1970-01-01 was a Thursday.
    let weekday = ((days + 4) % 7) as usize;
    let (year, month, day) = civil_from_days(days);

    out.extend_from_slice(DAYS[weekday]);
    out.extend_from_slice(b", ");
    two(day, out);
    out.push(b' ');
    out.extend_from_slice(MONTHS[month as usize - 1]);
    out.push(b' ');
    out.extend_from_slice(year.to_string().as_bytes());
    out.push(b' ');
    two(secs / 3600, out);
    out.push(b':');
    two(secs / 60 % 60, out);
    out.push(b':');
    two(secs % 60, out);
    out.extend_from_slice(b" GMT");
}

fn two(v: u64, out: &mut Vec<u8>) {
    out.push(b'0' + (v / 10 % 10) as u8);
    out.push(b'0' + (v % 10) as u8);
}

/// Year, month (1..=12) and day (1..=31) of a day count since 1970-01-01.
fn civil_from_days(days: u64) -> (u64, u64, u64) {
    let z = days + 719_468;
    let era = z / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + u64::from(month <= 2);
    (year, month, day)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fmt(secs: u64) -> String {
        let mut out = Vec::new();
        imf_fixdate(secs, &mut out);
        String::from_utf8(out).unwrap()
    }

    #[test]
    fn the_epoch_and_a_leap_day() {
        assert_eq!(fmt(0), "Thu, 01 Jan 1970 00:00:00 GMT");
        // 2000-02-29T23:59:59Z, a leap day of a century divisible by 400.
        assert_eq!(fmt(951_868_799), "Tue, 29 Feb 2000 23:59:59 GMT");
        // 2100-03-01, after a century year that is not a leap year.
        assert_eq!(fmt(4_107_542_400), "Mon, 01 Mar 2100 00:00:00 GMT");
    }
}
