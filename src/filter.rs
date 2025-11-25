use pyo3::prelude::*;

#[pyclass]
pub struct BloomFilter {
    insertions: usize,
    bits_mask: usize,
    slice_count: usize,
    bits: Vec<u64>,
    additions: usize,
}

#[pymethods]
impl BloomFilter {
    #[new]
    fn new(insertions: usize, fpp: f64) -> Self {
        // Input validation
        let insertions = if insertions == 0 { 1 } else { insertions };

        // Clamp FPP to valid range [0.001, 0.999]
        let fpp = fpp.max(0.001).min(0.999);

        let ln2 = 2f64.ln();
        let factor = -fpp.ln() / (ln2 * ln2);
        let mut bits = ((insertions as f64 * factor) as usize).next_power_of_two();
        if bits == 0 {
            bits = 1
        }

        let slice_count = ((ln2 * bits as f64 / insertions as f64) as usize).max(1);

        log::debug!(
            "BloomFilter created: insertions={}, fpp={}, bits={}, slice_count={}",
            insertions,
            fpp,
            bits,
            slice_count
        );

        Self {
            insertions,
            bits_mask: bits - 1,
            slice_count,
            bits: vec![0; bits.div_ceil(64)],
            additions: 0,
        }
    }

    pub fn put(&mut self, key: u64) {
        self.additions += 1;
        if self.additions >= self.insertions {
            self.reset();
        }

        for i in 0..self.slice_count {
            // Use wrapping arithmetic to prevent overflow
            let hash = key.wrapping_add((i as u64).wrapping_mul(key >> 32));
            let hash_index = (hash & self.bits_mask as u64) as usize;
            self.set(hash_index);
        }
    }

    fn get(&self, key: usize) -> bool {
        // Bounds checking
        if key >= self.bits.len() * 64 {
            log::warn!("BloomFilter get: key {} out of bounds", key);
            return false;
        }

        let idx = key >> 6;
        let offset = key & 63;
        let mask = 1u64 << offset;

        if idx >= self.bits.len() {
            return false;
        }

        let val = self.bits[idx];
        ((val & mask) >> offset) != 0
    }

    fn set(&mut self, key: usize) {
        // Bounds checking
        if key >= self.bits.len() * 64 {
            log::warn!("BloomFilter set: key {} out of bounds", key);
            return;
        }

        let idx = key >> 6;
        let offset = key & 63;

        if idx >= self.bits.len() {
            log::warn!(
                "BloomFilter set: computed idx {} exceeds bits length {}",
                idx,
                self.bits.len()
            );
            return;
        }

        let mask = 1u64 << offset;
        self.bits[idx] |= mask;
    }

    pub fn contains(&self, key: u64) -> bool {
        // Early exit if no hashes configured
        if self.slice_count == 0 {
            log::warn!(
                "BloomFilter contains: slice_count is 0, this indicates a configuration error"
            );
            return false;
        }

        let mut result = true;
        for i in 0..self.slice_count {
            // Use wrapping arithmetic to safely handle overflows
            let hash = key.wrapping_add((i as u64).wrapping_mul(key >> 32));
            let hash_index = (hash & self.bits_mask as u64) as usize;

            if !self.get(hash_index) {
                result = false;
                break;
            }
        }
        result
    }

    fn reset(&mut self) {
        self.bits = vec![0; self.bits.len()];
        self.additions = 0;
        log::debug!("BloomFilter reset: cleared all bits");
    }
}

#[cfg(test)]
mod tests {

    use super::BloomFilter;

    #[test]
    fn test_filter() {
        let mut bf = BloomFilter::new(100, 0.001);
        assert_eq!(bf.slice_count, 14);
        assert_eq!(bf.bits.len(), 32);
        for i in 0..100 {
            let exist = bf.contains(i);
            assert!(!exist);
            bf.put(i);
        }
        bf.reset();
        for i in 0..40 {
            let exist = bf.contains(i);
            assert!(!exist);
            bf.put(i);
        }
        // test exists
        for i in 0..40 {
            let exist = bf.contains(i);
            assert!(exist);
        }
    }

    #[test]
    fn test_filter_edge_cases() {
        // Test with zero insertions
        let mut bf = BloomFilter::new(0, 0.001);
        assert!(bf.insertions > 0);
        bf.put(1);

        // Test with invalid FPP
        let mut bf = BloomFilter::new(100, 0.0);
        bf.put(1);

        let mut bf = BloomFilter::new(100, 1.5);
        bf.put(1);

        // Test with large keys
        let mut bf = BloomFilter::new(100, 0.001);
        bf.put(u64::MAX);
        assert!(bf.contains(u64::MAX));
    }

    #[test]
    fn test_filter_bounds() {
        let mut bf = BloomFilter::new(100, 0.001);

        // These should not panic
        let _ = bf.contains(0);
        let _ = bf.contains(u64::MAX);

        bf.put(0);
        bf.put(u64::MAX);

        assert!(bf.contains(0));
        assert!(bf.contains(u64::MAX));
    }
}
