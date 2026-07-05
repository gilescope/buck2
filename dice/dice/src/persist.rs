/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! DICE graph snapshot persistence.
//!
//! Design: `docs/persistence_plan.md` + `docs/persistence_impl.md`. The
//! versioned graph already implements partial invalidation in memory;
//! persistence makes it durable. The division of labour with the existing
//! page-out machinery (`DiceStorage`):
//!
//! - Values ride the page-out path unchanged: content-addressed blobs in the
//!   `DiceStorage` backend, referenced by `DataKey`.
//! - This module persists what page-out does not: the graph *skeleton* -
//!   erased keys (via `pagable_typetag`), dep edges, `VersionRanges`,
//!   `ForceDirtyHistory` - into a metadata file beside the store.
//!
//! On load, nodes are reinstalled with their values paged out; the existing
//! worker hydration path brings values back on first demand. Injected leaves
//! are reinstalled hydrated, and the daemon's normal first-command
//! `changed_to` calls produce the dirty frontier - unchanged leaves compare
//! equal (no invalidation), changed ones propagate through the re-derived
//! rdep edges exactly as they would have in a warm daemon.
//!
//! Trust: the header carries an opaque `inputs_digest` (the embedder hashes
//! binary + config reality into it). Any mismatch, unknown type tag, or
//! decode failure degrades to recompute - corruption can cost time, never
//! correctness.

use std::path::Path;
use std::sync::Arc;

use dashmap::DashMap;
use dupe::Dupe;
use pagable::PagableSerialize;
use pagable::context::PagableDeserializerImpl;
use pagable::storage::handle::PagableStorageHandle;
use pagable::storage::support::SerializerForPaging;
use pagable::traits::PagableBoxDeserialize;
use serde::Deserialize;
use serde::Serialize;

use crate::api::key::InvalidationSourcePriority;
use crate::impls::core::graph::nodes::ForceDirtyHistory;
use crate::impls::core::graph::nodes::InjectedGraphNode;
use crate::impls::core::graph::nodes::OccupiedGraphNode;
use crate::impls::core::graph::nodes::VersionedGraphNode;
use crate::impls::core::state::CoreStateHandle;
use crate::impls::deps::graph::SeriesParallelDeps;
use crate::impls::key::CowDiceKeyHashed;
use crate::impls::key::DiceKey;
use crate::impls::key::DiceKeyErased;
use crate::impls::key_index::DiceKeyIndex;
use crate::impls::storage::DiceStorage;
use crate::impls::value::DiceValidValue;
use crate::versions::VersionNumber;
use crate::versions::VersionRange;
use crate::versions::VersionRanges;

/// Bump on ANY change to the persisted types below (bincode is sensitive to
/// field order and enum layout). Mismatch => cold start.
pub(crate) const SNAPSHOT_SCHEMA_VERSION: u32 = 2;

pub(crate) const SNAPSHOT_MAGIC: [u8; 8] = *b"DICE\0SNP";

/// Validated before any node bytes are read. Every field mismatch is a
/// silent cold start: persisted state is a cache, never truth.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotHeader {
    pub(crate) magic: [u8; 8],
    pub(crate) schema_version: u32,
    /// Opaque digest of everything the embedder considers identity-defining
    /// (binary hash, config digests). Cross-version reuse is a non-goal.
    pub inputs_digest: [u8; 32],
    pub(crate) node_count: u64,
    /// The version the snapshot was taken at (V_persist).
    pub(crate) max_version: u64,
}

impl SnapshotHeader {
    pub(crate) fn new(inputs_digest: [u8; 32], node_count: u64, max_version: u64) -> Self {
        SnapshotHeader {
            magic: SNAPSHOT_MAGIC,
            schema_version: SNAPSHOT_SCHEMA_VERSION,
            inputs_digest,
            node_count,
            max_version,
        }
    }

    pub(crate) fn matches(&self, inputs_digest: &[u8; 32]) -> bool {
        self.magic == SNAPSHOT_MAGIC
            && self.schema_version == SNAPSHOT_SCHEMA_VERSION
            && &self.inputs_digest == inputs_digest
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
                priority: priority_to_wire(priority),
            },
        }
    }

    pub(crate) fn into_internal(self) -> anyhow::Result<ForceDirtyHistory> {
        Ok(ForceDirtyHistory::from_persisted_parts(
            self.versions
                .into_iter()
                .map(|v| VersionNumber::new(v as usize))
                .collect(),
            priority_from_wire(self.priority)?,
        ))
    }
}

fn priority_to_wire(p: InvalidationSourcePriority) -> u8 {
    match p {
        InvalidationSourcePriority::Ignored => 0,
        InvalidationSourcePriority::Normal => 1,
        InvalidationSourcePriority::High => 2,
    }
}

fn priority_from_wire(p: u8) -> anyhow::Result<InvalidationSourcePriority> {
    Ok(match p {
        0 => InvalidationSourcePriority::Ignored,
        1 => InvalidationSourcePriority::Normal,
        2 => InvalidationSourcePriority::High,
        other => {
            return Err(anyhow::anyhow!(
                "corrupt snapshot: invalidation priority {other} (expected 0..=2)"
            ));
        }
    })
}

/// Plain-data extraction of one graph node, produced on the state thread.
pub(crate) enum PersistNodeExtract {
    Occupied {
        key: DiceKey,
        deps: Vec<DiceKey>,
        verified_ranges: VersionRanges,
        dirtied_history: ForceDirtyHistory,
        value: PersistValueExtract,
    },
    Injected {
        key: DiceKey,
        first_valid_version: VersionNumber,
        value: DiceValidValue,
    },
}

pub(crate) enum PersistValueExtract {
    /// Already in the store (paged out) - just reference it.
    Paged(pagable::DataKey),
    /// Resident only; the saver serializes it (or drops the node).
    Hydrated(DiceValidValue),
}

/// One node in the metadata file. Blobs live in the `DiceStorage` backend,
/// referenced by content-addressed `DataKey` (u128).
#[derive(Debug, Clone, Serialize, Deserialize)]
enum Record {
    Occupied {
        key_blob: u128,
        /// Ordinals into the record list. `u64::MAX` = the dep could not be
        /// persisted; the loader drops this record (dep will recompute, and
        /// this node will follow via CheckDeps).
        deps: Vec<u64>,
        verified_ranges: PersistedVersionRanges,
        dirtied_history: PersistedForceDirty,
        value_blob: u128,
    },
    Injected {
        key_blob: u128,
        first_valid_version: u64,
        /// Wire form of the key's `InvalidationSourcePriority`.
        priority: u8,
        value_blob: u128,
    },
    /// Key interned for dep-edge resolution, but no node installed (its
    /// value could not be serialized). Dependents stay alive: the dep
    /// recomputes on demand and equality-based reuse may still rescue the
    /// subtree above it.
    KeyOnly { key_blob: u128 },
}

impl Record {
    fn key_blob(&self) -> u128 {
        match self {
            Record::Occupied { key_blob, .. }
            | Record::Injected { key_blob, .. }
            | Record::KeyOnly { key_blob } => *key_blob,
        }
    }

    /// Canonical ordering for byte-stable output: kind tag, then
    /// content-addressed key blob.
    fn sort_key(&self) -> (u8, u128) {
        match self {
            Record::Occupied { key_blob, .. } => (0, *key_blob),
            Record::Injected { key_blob, .. } => (1, *key_blob),
            Record::KeyOnly { key_blob } => (2, *key_blob),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct MetaFile {
    header: SnapshotHeader,
    records: Vec<Record>,
}

/// Counters describing what a save or load actually did. `dropped_*` are the
/// no-silent-caps ledger: every node that fell out of the snapshot is
/// accounted for.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct PersistStats {
    pub nodes_persisted: usize,
    pub nodes_injected: usize,
    pub keys_only: usize,
    /// Node dropped because its key or value has no serialization.
    pub dropped_unserializable: usize,
    /// Node dropped at load because a dep could not be persisted.
    pub dropped_dangling_dep: usize,
}

fn encode<T: Serialize>(value: &T) -> anyhow::Result<Vec<u8>> {
    Ok(bincode::serde::encode_to_vec(
        value,
        bincode::config::standard(),
    )?)
}

fn decode<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> anyhow::Result<T> {
    let (value, consumed) = bincode::serde::decode_from_slice(bytes, bincode::config::standard())?;
    if consumed != bytes.len() {
        return Err(anyhow::anyhow!(
            "corrupt snapshot: {} trailing bytes after decode",
            bytes.len() - consumed
        ));
    }
    Ok(value)
}

/// Serialize an erased key (tag + body via `pagable_typetag`) into the store.
/// Returns None for keys with no registered serialization (projections are
/// not yet supported - S2).
fn store_key_blob(
    storage: &DiceStorage,
    key: &DiceKeyErased,
    finished: &DashMap<usize, Arc<pagable::storage::traits::ArcSerSlot>>,
) -> Option<pagable::DataKey> {
    let DiceKeyErased::Key(k) = key else {
        return None; // Projection keys: S2.
    };
    let session_context = storage.storage().session_context();
    let mut ser = SerializerForPaging::new(session_context);
    // A key type whose fields lack Pagable impls fails here; treat as
    // unpersistable rather than fatal.
    match <dyn crate::impls::key::DiceKeyDyn as PagableSerialize>::pagable_serialize(
        k.as_ref(),
        &mut ser,
    ) {
        Ok(()) => {}
        Err(_) => return None,
    }
    let (data, arcs) = ser.finish();
    storage
        .storage()
        .page_out_item(data, arcs, finished, session_context)
        .ok()
}

/// Save the current graph as a snapshot: values into the `DiceStorage`
/// backend, skeleton into `meta_path`. Caller must ensure DICE is idle and
/// should run `Dice::page_out()` first so most values already have
/// `DataKey`s.
pub(crate) async fn save_snapshot(
    state: &CoreStateHandle,
    key_index: &DiceKeyIndex,
    storage: &DiceStorage,
    meta_path: &Path,
    inputs_digest: [u8; 32],
) -> anyhow::Result<PersistStats> {
    let (version, extracts) = state.persist_extract().await;
    let mut stats = PersistStats::default();
    let finished = DashMap::new();

    // First pass: serialize keys and values, build records keyed by DiceKey.
    let mut records: Vec<(DiceKey, Record)> = Vec::with_capacity(extracts.len());
    let mut persistable: std::collections::HashMap<DiceKey, bool> =
        std::collections::HashMap::new();

    for extract in &extracts {
        let (key, ok) = match extract {
            PersistNodeExtract::Occupied { key, value, .. } => {
                let key_erased = key_index.get(*key);
                let key_blob = store_key_blob(storage, key_erased, &finished);
                let value_blob = match (key_blob, value) {
                    (None, _) => None,
                    (Some(_), PersistValueExtract::Paged(dk)) => Some(*dk),
                    (Some(_), PersistValueExtract::Hydrated(v)) => {
                        storage.store_value_blob(key_erased, v.dupe(), &finished)?
                    }
                };
                match (key_blob, value_blob) {
                    (Some(kb), Some(vb)) => {
                        records.push((
                            *key,
                            Record::Occupied {
                                key_blob: kb.0,
                                deps: Vec::new(), // filled after ordinals exist
                                verified_ranges: PersistedVersionRanges::from_internal(
                                    match extract {
                                        PersistNodeExtract::Occupied {
                                            verified_ranges, ..
                                        } => verified_ranges,
                                        _ => unreachable!(),
                                    },
                                ),
                                dirtied_history: PersistedForceDirty::from_internal(
                                    match extract {
                                        PersistNodeExtract::Occupied {
                                            dirtied_history, ..
                                        } => dirtied_history,
                                        _ => unreachable!(),
                                    },
                                ),
                                value_blob: vb.0,
                            },
                        ));
                        (*key, true)
                    }
                    (Some(kb), None) => {
                        // Value unserializable: keep the key for dep edges.
                        records.push((*key, Record::KeyOnly { key_blob: kb.0 }));
                        stats.keys_only += 1;
                        (*key, true)
                    }
                    (None, _) => {
                        stats.dropped_unserializable += 1;
                        (*key, false)
                    }
                }
            }
            PersistNodeExtract::Injected {
                key,
                first_valid_version,
                value,
            } => {
                let key_erased = key_index.get(*key);
                let key_blob = store_key_blob(storage, key_erased, &finished);
                let value_blob = match key_blob {
                    None => None,
                    Some(_) => storage.store_value_blob(key_erased, value.dupe(), &finished)?,
                };
                let priority = key_erased.invalidation_source_priority();
                match (key_blob, value_blob) {
                    (Some(kb), Some(vb)) => {
                        records.push((
                            *key,
                            Record::Injected {
                                key_blob: kb.0,
                                first_valid_version: first_valid_version.value() as u64,
                                priority: priority_to_wire(priority),
                                value_blob: vb.0,
                            },
                        ));
                        (*key, true)
                    }
                    _ => {
                        stats.dropped_unserializable += 1;
                        (*key, false)
                    }
                }
            }
        };
        persistable.insert(key, ok);
    }

    // Canonical order (kind, content-hash) => byte-stable metadata for
    // identical graphs regardless of HashMap iteration order.
    records.sort_by_key(|(_, r)| r.sort_key());
    let ordinal_of: std::collections::HashMap<DiceKey, u64> = records
        .iter()
        .enumerate()
        .map(|(i, (k, _))| (*k, i as u64))
        .collect();

    // Second pass: resolve dep ordinals.
    for extract in &extracts {
        let PersistNodeExtract::Occupied { key, deps, .. } = extract else {
            continue;
        };
        let Some(ordinal) = ordinal_of.get(key) else {
            continue; // node was dropped
        };
        let dep_ordinals: Vec<u64> = deps
            .iter()
            .map(|d| ordinal_of.get(d).copied().unwrap_or(u64::MAX))
            .collect();
        if let Record::Occupied { deps: slot, .. } = &mut records[*ordinal as usize].1 {
            *slot = dep_ordinals;
        }
    }

    stats.nodes_persisted = records
        .iter()
        .filter(|(_, r)| matches!(r, Record::Occupied { .. }))
        .count();
    stats.nodes_injected = records
        .iter()
        .filter(|(_, r)| matches!(r, Record::Injected { .. }))
        .count();

    storage.storage().flush()?;

    let meta = MetaFile {
        header: SnapshotHeader::new(inputs_digest, records.len() as u64, version.value() as u64),
        records: records.into_iter().map(|(_, r)| r).collect(),
    };
    let bytes = encode(&meta)?;
    let tmp = meta_path.with_extension("tmp");
    std::fs::write(&tmp, &bytes)?;
    std::fs::rename(&tmp, meta_path)?;
    Ok(stats)
}

/// Load a snapshot into a freshly-built DICE. Returns `Ok(None)` (cold
/// start) on missing file or header mismatch; per-record failures drop the
/// record and continue.
pub(crate) async fn load_snapshot(
    state: &CoreStateHandle,
    key_index: &DiceKeyIndex,
    storage: &DiceStorage,
    meta_path: &Path,
    inputs_digest: [u8; 32],
) -> anyhow::Result<Option<PersistStats>> {
    let bytes = match std::fs::read(meta_path) {
        Ok(b) => b,
        Err(_) => return Ok(None),
    };
    let meta: MetaFile = match decode(&bytes) {
        Ok(m) => m,
        Err(_) => return Ok(None),
    };
    if !meta.header.matches(&inputs_digest) {
        return Ok(None);
    }
    let at_version = VersionNumber::new(meta.header.max_version as usize);
    let mut stats = PersistStats::default();

    // Pass 1: fetch + deserialize + intern every key. A failed key (unknown
    // tag after a code change, corrupt blob) leaves None; nodes referencing
    // it are dropped.
    let handle = PagableStorageHandle::new(storage.storage().dupe());
    let mut dice_keys: Vec<Option<DiceKey>> = Vec::with_capacity(meta.records.len());
    let mut erased_keys: Vec<Option<DiceKeyErased>> = Vec::with_capacity(meta.records.len());
    for record in &meta.records {
        let data_key = pagable::DataKey(record.key_blob());
        let loaded = match storage.storage().fetch_data(&data_key).await {
            Ok(data) => {
                let mut deser = PagableDeserializerImpl::new(&data.data, &data.arcs, &handle);
                match <dyn crate::impls::key::DiceKeyDyn as PagableBoxDeserialize>::deserialize_box(
                    &mut deser,
                ) {
                    Ok(boxed) => {
                        let erased = DiceKeyErased::Key(Arc::from(boxed));
                        let dice_key =
                            key_index.index(CowDiceKeyHashed::from_erased(erased.dupe()));
                        Some((dice_key, erased))
                    }
                    Err(_) => None,
                }
            }
            Err(_) => None,
        };
        match loaded {
            Some((dk, er)) => {
                dice_keys.push(Some(dk));
                erased_keys.push(Some(er));
            }
            None => {
                stats.dropped_unserializable += 1;
                dice_keys.push(None);
                erased_keys.push(None);
            }
        }
    }

    // Pass 2: build nodes.
    let mut nodes: Vec<(DiceKey, VersionedGraphNode)> = Vec::with_capacity(meta.records.len());
    for (i, record) in meta.records.iter().enumerate() {
        let Some(dice_key) = dice_keys[i] else {
            continue;
        };
        match record {
            Record::KeyOnly { .. } => {
                stats.keys_only += 1;
            }
            Record::Occupied {
                deps,
                verified_ranges,
                dirtied_history,
                value_blob,
                ..
            } => {
                let mut dep_keys = Vec::with_capacity(deps.len());
                let mut dangling = false;
                for d in deps {
                    match usize::try_from(*d)
                        .ok()
                        .and_then(|ix| dice_keys.get(ix).copied().flatten())
                    {
                        Some(k) => dep_keys.push(k),
                        None => {
                            dangling = true;
                            break;
                        }
                    }
                }
                if dangling {
                    stats.dropped_dangling_dep += 1;
                    continue;
                }
                let node = OccupiedGraphNode::new_paged_out_for_persist(
                    dice_key,
                    pagable::DataKey(*value_blob),
                    crate::arc::Arc::new(SeriesParallelDeps::serial_from_vec(dep_keys)),
                    verified_ranges.clone().into_internal(),
                    dirtied_history.clone().into_internal()?,
                );
                nodes.push((dice_key, VersionedGraphNode::Occupied(node)));
                stats.nodes_persisted += 1;
            }
            Record::Injected {
                first_valid_version,
                priority,
                value_blob,
                ..
            } => {
                // Injected leaves hydrate eagerly: their values are the
                // baseline the first command's changed_to calls diff against.
                let erased = erased_keys[i].as_ref().unwrap();
                let value = match storage.hydrate(erased, pagable::DataKey(*value_blob)).await {
                    Ok(v) => v,
                    Err(_) => {
                        stats.dropped_unserializable += 1;
                        continue;
                    }
                };
                let node = InjectedGraphNode::new(
                    dice_key,
                    VersionNumber::new(*first_valid_version as usize),
                    value,
                    priority_from_wire(*priority)?,
                );
                nodes.push((dice_key, VersionedGraphNode::Injected(node)));
                stats.nodes_injected += 1;
            }
        }
    }

    state.persist_install(nodes, at_version).await;
    Ok(Some(stats))
}
