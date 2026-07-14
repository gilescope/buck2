/*
 * Copyright 2019 The Starlark in Rust Authors.
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! Duplicate-string attribution across frozen heaps.
//!
//! Sizes the win of freeze-time string interning (L1 of buck2's dice
//! tail-memory plan): every module allocates its own copy of common strings
//! (`"-Copt-level=3"`, ...), so the reclaimable fraction is
//! `total_bytes - distinct_bytes`.

use std::collections::HashMap;

use crate::values::layout::heap::heap_type::FrozenHeapRef;

/// One duplicated string, from [`dup_string_stats`].
pub struct DupStringStat {
    /// The string's content.
    pub sample: String,
    /// Payload length in bytes (excludes per-value header/alignment).
    pub len: usize,
    /// Copies across all walked heaps.
    pub copies: usize,
}

impl DupStringStat {
    /// Bytes reclaimable by collapsing all copies to one allocation.
    pub fn wasted_bytes(&self) -> usize {
        self.len * (self.copies - 1)
    }
}

/// Aggregate duplicate-string stats, from [`dup_string_stats`].
#[derive(Default)]
pub struct DupStringStats {
    /// Heaps walked.
    pub heaps: usize,
    /// String values across all heaps.
    pub total_strings: usize,
    /// Payload bytes across all heaps (excludes headers/alignment).
    pub total_bytes: usize,
    /// Distinct string contents (by 64-bit content hash - collisions are
    /// negligible at this scale, see [`dup_string_stats`]).
    pub distinct_strings: usize,
    /// Payload bytes after collapsing duplicates - `total_bytes` minus this
    /// is the interning ceiling.
    pub distinct_bytes: usize,
    /// Worst offenders by wasted bytes, descending; ties broken by content
    /// so output is deterministic.
    pub top: Vec<DupStringStat>,
}

/// FNV-1a: deterministic (unlike `RandomState`) and cheap. 64-bit collisions
/// at millions of strings shift stats immeasurably - fine for attribution.
fn fnv1a(s: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in s.as_bytes() {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x100_0000_01b3);
    }
    h
}

/// Compute duplicate-string stats over `heaps` (each heap's own strings only;
/// pass every heap of interest - `refs()` are not followed, which is correct
/// when the input is the full live-heap registry since every non-empty
/// frozen heap registers itself). Two passes: count by content hash, then re-walk to
/// attach content to the top `top_n` offenders - avoids retaining a copy of
/// every distinct string.
pub fn dup_string_stats(heaps: &[FrozenHeapRef], top_n: usize) -> DupStringStats {
    // hash -> (payload len, copies)
    let mut counts: HashMap<u64, (usize, usize)> = HashMap::new();
    let mut total_strings = 0usize;
    let mut total_bytes = 0usize;
    for heap in heaps {
        heap.for_each_string(|s| {
            total_strings += 1;
            total_bytes += s.len();
            let e = counts.entry(fnv1a(s)).or_insert((s.len(), 0));
            e.1 += 1;
        });
    }
    let distinct_strings = counts.len();
    let distinct_bytes = counts.values().map(|(len, _)| len).sum();

    let mut dupes: Vec<(u64, usize, usize)> = counts
        .iter()
        .filter(|(_, (_, copies))| *copies > 1)
        .map(|(h, (len, copies))| (*h, *len, *copies))
        .collect();
    dupes.sort_by(|a, b| {
        ((b.2 - 1) * b.1)
            .cmp(&((a.2 - 1) * a.1))
            .then(b.1.cmp(&a.1))
    });
    dupes.truncate(top_n);

    let mut samples: HashMap<u64, String> = HashMap::with_capacity(dupes.len());
    if !dupes.is_empty() {
        let wanted: std::collections::HashSet<u64> = dupes.iter().map(|d| d.0).collect();
        for heap in heaps {
            heap.for_each_string(|s| {
                let h = fnv1a(s);
                if wanted.contains(&h) {
                    samples.entry(h).or_insert_with(|| s.to_owned());
                }
            });
        }
    }

    let mut top: Vec<DupStringStat> = dupes
        .into_iter()
        .map(|(h, len, copies)| DupStringStat {
            sample: samples.remove(&h).unwrap_or_default(),
            len,
            copies,
        })
        .collect();
    // Wasted-desc then content: HashMap iteration order must not leak out.
    top.sort_by(|a, b| {
        b.wasted_bytes()
            .cmp(&a.wasted_bytes())
            .then_with(|| a.sample.cmp(&b.sample))
    });

    DupStringStats {
        heaps: heaps.len(),
        total_strings,
        total_bytes,
        distinct_strings,
        distinct_bytes,
        top,
    }
}

#[cfg(test)]
mod tests {
    use super::dup_string_stats;
    use crate::values::FrozenHeap;

    #[test]
    fn test_dup_string_stats() {
        // Note a single FrozenHeap interns its own strings, so duplication
        // only exists ACROSS heaps - exactly what L1 interning would collapse.
        let a = FrozenHeap::new();
        a.alloc("shared-flag-string");
        a.alloc("only-in-a");
        let b = FrozenHeap::new();
        b.alloc("shared-flag-string");
        b.alloc("only-in-b!");
        let c = FrozenHeap::new();
        c.alloc("shared-flag-string");
        let heaps = [
            a.into_ref_impl(None, None),
            b.into_ref_impl(None, None),
            c.into_ref_impl(None, None),
        ];

        let stats = dup_string_stats(&heaps, 10);
        assert_eq!(stats.heaps, 3);
        assert_eq!(stats.total_strings, 5);
        assert_eq!(stats.total_bytes, 18 + 9 + 18 + 18 + 10);
        assert_eq!(stats.distinct_strings, 3);
        assert_eq!(stats.distinct_bytes, 18 + 9 + 10);
        assert_eq!(stats.top.len(), 1);
        let t = &stats.top[0];
        assert_eq!(
            (t.sample.as_str(), t.len, t.copies),
            ("shared-flag-string", 18, 3)
        );
        assert_eq!(t.wasted_bytes(), 36);
    }

    #[test]
    fn test_dup_string_stats_empty() {
        let stats = dup_string_stats(&[], 10);
        assert_eq!(stats.total_strings, 0);
        assert_eq!(stats.distinct_bytes, 0);
        assert!(stats.top.is_empty());
    }
}
