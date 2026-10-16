// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::hash::{BuildHasherDefault, Hasher};

/// A low-cost hasher for numeric identities and pointer addresses that are already unique.
///
/// Sequential IDs and nearby pointers tend to differ in only a small region:
///
/// ```text
/// 0x0000000012341234
/// 0x0000000056785678
/// 0x0001C00B59595959
/// 0x0001C00B59636363
/// ```
///
/// This can lead to unbalanced hash tables. Combining the value with its bit-reversed form spreads
/// the distinguishing bits across the result without the cost of a general-purpose keyed hash.
#[derive(Debug, Default)]
pub(crate) struct NumericHasher {
    value: u64,
}

impl Hasher for NumericHasher {
    #[cfg_attr(test, mutants::skip)] // Difficult to test without getting silly and hardcoding results.
    fn finish(&self) -> u64 {
        self.value
    }

    #[cfg_attr(test, mutants::skip)] // Difficult to test without getting silly and hardcoding results.
    fn write(&mut self, bytes: &[u8]) {
        let input_raw = if let Ok(bytes) = <[u8; 8]>::try_from(bytes) {
            u64::from_ne_bytes(bytes)
        } else {
            u64::from(u32::from_ne_bytes(
                bytes
                    .try_into()
                    .expect("NumericHasher input must be a 32-bit or 64-bit numeric identity"),
            ))
        };

        self.value = self.value.rotate_left(17) ^ input_raw ^ input_raw.reverse_bits();
    }
}

/// Builder for [`NumericHasher`].
pub(crate) type BuildNumericHasher = BuildHasherDefault<NumericHasher>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_same() {
        let mut hasher1 = NumericHasher::default();
        let mut hasher2 = NumericHasher::default();

        hasher1.write(&1u64.to_ne_bytes());
        hasher2.write(&1u64.to_ne_bytes());

        assert_eq!(hasher1.finish(), hasher2.finish());
    }

    #[test]
    fn accepts_32_bit_input() {
        let mut hasher = NumericHasher::default();

        hasher.write(&1u32.to_ne_bytes());

        assert_ne!(hasher.finish(), 0);
    }

    #[test]
    #[should_panic(expected = "NumericHasher input must be a 32-bit or 64-bit numeric identity")]
    fn rejects_other_input_widths() {
        NumericHasher::default().write(&[1]);
    }
}
