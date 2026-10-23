//! Latency statistics of the video rows.

/// The nearest-rank `p`th percentile (0..=100) of `sorted`; 0 when empty.
pub fn percentile(sorted: &[u64], p: u32) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = (u64::from(p.min(100)) * sorted.len() as u64).div_ceil(100);
    sorted[(rank.max(1) - 1) as usize]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentile_is_nearest_rank() {
        let values: Vec<u64> = (1..=100).collect();
        assert_eq!(percentile(&values, 50), 50);
        assert_eq!(percentile(&values, 99), 99);
        assert_eq!(percentile(&values, 100), 100);
        assert_eq!(percentile(&[7, 9], 50), 7);
        assert_eq!(percentile(&[], 50), 0);
    }
}
