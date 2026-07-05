/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! DICE snapshot persistence (S1 core: header + graph-metadata codecs).
//!
//! Design: `docs/persistence_plan.md` + `docs/persistence_impl.md`. The
//! versioned graph already implements partial invalidation; persistence makes
//! the version history durable so a fresh process can reload it and dirty
//! only the injected leaves that actually changed.
//!
//! This module currently provides the wire format and the codecs for the two
//! correctness-critical metadata types (`VersionRanges`,
//! `ForceDirtyHistory`). Graph iteration (save) and node reconstruction
//! (load) build on these next.
//!
//! Encoding is bincode 2 (serde, standard config): deterministic for a fixed
//! schema. Any structural change to persisted types MUST bump
//! `SNAPSHOT_SCHEMA_VERSION`; a header mismatch is a silent cold start,
//! never an error.

#![allow(dead_code)] // S1 scaffolding: wired to the graph in the next stage.

use serde::Deserialize;
use serde::Serialize;

use crate::api::key::InvalidationSourcePriority;
use crate::impls::core::graph::nodes::ForceDirtyHistory;
use crate::versions::VersionNumber;
use crate::versions::VersionRange;
use crate::versions::VersionRanges;

/// Bump on ANY change to the persisted types below (bincode is sensitive to
/// field order and enum layout). Mismatch => cold start.
pub(crate) const SNAPSHOT_SCHEMA_VERSION: u32 = 1;

pub(crate) const SNAPSHOT_MAGIC: [u8; 8] = *b"DICE\0SNP";

/// Validated before any node bytes are read. Every field mismatch is a
/// silent cold start: persisted state is a cache, never truth.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SnapshotHeader {
    pub(crate) magic: [u8; 8],
    pub(crate) schema_version: u32,
    /// sha256 of the producing binary — cross-version reuse is a non-goal.
    pub(crate) binary_hash: [u8; 32],
    pub(crate) buckconfig_digest: [u8; 32],
    pub(crate) cell_digest: [u8; 32],
    pub(crate) prelude_digest: [u8; 32],
    pub(crate) node_count: u64,
    /// The version the snapshot was taken at (V_persist).
    pub(crate) max_version: u64,
}

/// Why a snapshot was rejected. Diagnostics only — callers cold-start
/// identically for every variant.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum HeaderMismatch {
    Magic,
    Schema,
    BinaryHash,
    BuckconfigDigest,
    CellDigest,
    PreludeDigest,
}

impl SnapshotHeader {
    pub(crate) fn validate(&self, expected: &SnapshotHeader) -> Result<(), HeaderMismatch> {
        if self.magic != SNAPSHOT_MAGIC {
            return Err(HeaderMismatch::Magic);
        }
        if self.schema_version != expected.schema_version {
            return Err(HeaderMismatch::Schema);
        }
        if self.binary_hash != expected.binary_hash {
            return Err(HeaderMismatch::BinaryHash);
        }
        if self.buckconfig_digest != expected.buckconfig_digest {
            return Err(HeaderMismatch::BuckconfigDigest);
        }
        if self.cell_digest != expected.cell_digest {
            return Err(HeaderMismatch::CellDigest);
        }
        if self.prelude_digest != expected.prelude_digest {
            return Err(HeaderMismatch::PreludeDigest);
        }
        Ok(())
    }
}

/// `VersionRanges` on the wire: end-exclusive `[begin, end)` intervals,
/// `end = None` for the open final range.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PersistedVersionRanges(Vec<(u64, Option<u64>)>);

impl PersistedVersionRanges {
    pub(crate) fn from_internal(ranges: &VersionRanges) -> Self {
        PersistedVersionRanges(
            ranges
                .ranges_for_persist()
                .iter()
                .map(|r| {
                    let (b, e) = r.parts_for_persist();
                    (b.value() as u64, e.map(|e| e.value() as u64))
                })
                .collect(),
        )
    }

    pub(crate) fn into_internal(self) -> VersionRanges {
        VersionRanges::from_persisted_ranges(
            self.0
                .into_iter()
                .map(|(b, e)| {
                    VersionRange::from_persisted_parts(
                        VersionNumber::new(b as usize),
                        e.map(|e| VersionNumber::new(e as usize)),
                    )
                })
                .collect(),
        )
    }
}

/// `ForceDirtyHistory` on the wire. Load-bearing: the `restricted_range`
/// invariant depends on every marker surviving verbatim, priority included.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PersistedForceDirty {
    versions: Vec<u64>,
    /// 0 = Ignored, 1 = Normal, 2 = High. Explicit numeric mapping so the
    /// wire format cannot drift if the enum is reordered.
    priority: u8,
}

impl PersistedForceDirty {
    pub(crate) fn from_internal(history: &ForceDirtyHistory) -> Self {
        match history.parts_for_persist() {
            None => PersistedForceDirty {
                versions: Vec::new(),
                priority: 1,
            },
            Some((versions, priority)) => PersistedForceDirty {
                versions: versions.iter().map(|v| v.value() as u64).collect(),
                priority: match priority {
                    InvalidationSourcePriority::Ignored => 0,
                    InvalidationSourcePriority::Normal => 1,
                    InvalidationSourcePriority::High => 2,
                },
            },
        }
    }

    pub(crate) fn into_internal(self) -> anyhow::Result<ForceDirtyHistory> {
        let priority = match self.priority {
            0 => InvalidationSourcePriority::Ignored,
            1 => InvalidationSourcePriority::Normal,
            2 => InvalidationSourcePriority::High,
            other => {
                return Err(anyhow::anyhow!(
                    "corrupt snapshot: invalidation priority {other} (expected 0..=2)"
                ));
            }
        };
        Ok(ForceDirtyHistory::from_persisted_parts(
            self.versions
                .into_iter()
                .map(|v| VersionNumber::new(v as usize))
                .collect(),
            priority,
        ))
    }
}

pub(crate) fn encode<T: Serialize>(value: &T) -> anyhow::Result<Vec<u8>> {
    Ok(bincode::serde::encode_to_vec(
        value,
        bincode::config::standard(),
    )?)
}

pub(crate) fn decode<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> anyhow::Result<T> {
    let (value, consumed) = bincode::serde::decode_from_slice(bytes, bincode::config::standard())?;
    if consumed != bytes.len() {
        return Err(anyhow::anyhow!(
            "corrupt snapshot: {} trailing bytes after decode",
            bytes.len() - consumed
        ));
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header(schema: u32, binary: u8) -> SnapshotHeader {
        SnapshotHeader {
            magic: SNAPSHOT_MAGIC,
            schema_version: schema,
            binary_hash: [binary; 32],
            buckconfig_digest: [2; 32],
            cell_digest: [3; 32],
            prelude_digest: [4; 32],
            node_count: 42,
            max_version: 7,
        }
    }

    #[test]
    fn header_round_trip_and_validation() {
        let h = header(SNAPSHOT_SCHEMA_VERSION, 1);
        let decoded: SnapshotHeader = decode(&encode(&h).unwrap()).unwrap();
        assert_eq!(h, decoded);
        assert_eq!(decoded.validate(&h), Ok(()));

        let wrong_binary = header(SNAPSHOT_SCHEMA_VERSION, 9);
        assert_eq!(
            decoded.validate(&wrong_binary),
            Err(HeaderMismatch::BinaryHash)
        );
        let wrong_schema = header(SNAPSHOT_SCHEMA_VERSION + 1, 1);
        assert_eq!(decoded.validate(&wrong_schema), Err(HeaderMismatch::Schema));
    }

    #[test]
    fn version_ranges_round_trip() {
        let mut vr = VersionRanges::new();
        vr.insert(VersionRange::bounded(
            VersionNumber::new(1),
            VersionNumber::new(4),
        ));
        vr.insert(VersionRange::bounded(
            VersionNumber::new(9),
            VersionNumber::new(12),
        ));
        vr.insert(VersionRange::begins_with(VersionNumber::new(20)));

        let wire = PersistedVersionRanges::from_internal(&vr);
        let decoded: PersistedVersionRanges = decode(&encode(&wire).unwrap()).unwrap();
        assert_eq!(wire, decoded);
        assert_eq!(decoded.into_internal(), vr);
    }

    #[test]
    fn force_dirty_round_trip_preserves_semantics() {
        let mut fd = ForceDirtyHistory::new();
        fd.force_dirty(VersionNumber::new(3), InvalidationSourcePriority::High);
        fd.force_dirty(VersionNumber::new(8), InvalidationSourcePriority::High);
        fd.force_dirty(VersionNumber::new(21), InvalidationSourcePriority::High);

        let wire = PersistedForceDirty::from_internal(&fd);
        let decoded: PersistedForceDirty = decode(&encode(&wire).unwrap()).unwrap();
        let rebuilt = decoded.into_internal().unwrap();

        // Structural identity implies behavioral identity: restricted_range
        // is a pure function of (versions, priority).
        assert_eq!(
            fd.parts_for_persist().map(|(v, p)| (v.to_vec(), p)),
            rebuilt.parts_for_persist().map(|(v, p)| (v.to_vec(), p)),
        );
    }

    #[test]
    fn force_dirty_empty_round_trip() {
        let fd = ForceDirtyHistory::new();
        let rebuilt = PersistedForceDirty::from_internal(&fd)
            .into_internal()
            .unwrap();
        assert!(rebuilt.parts_for_persist().is_none());
    }

    #[test]
    fn corrupt_priority_is_an_error_not_a_panic() {
        let wire = PersistedForceDirty {
            versions: vec![1],
            priority: 7,
        };
        assert!(wire.into_internal().is_err());
    }

    /// The determinism gate, in miniature: encode -> decode -> encode must be
    /// byte-identical. The full save/load/save integration test extends this
    /// to whole snapshots before S1 merges.
    #[test]
    fn encode_is_deterministic_across_round_trip() {
        let mut vr = VersionRanges::new();
        for i in 0..50u32 {
            vr.insert(VersionRange::bounded(
                VersionNumber::new((i * 3) as usize),
                VersionNumber::new((i * 3 + 2) as usize),
            ));
        }
        let wire = PersistedVersionRanges::from_internal(&vr);
        let a = encode(&wire).unwrap();
        let decoded: PersistedVersionRanges = decode(&a).unwrap();
        let b = encode(&decoded).unwrap();
        assert_eq!(a, b, "serialization must be deterministic");
    }
}
