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
pub(crate) const SNAPSHOT_SCHEMA_VERSION: u32 = 3;

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
enum PersistedKey {
    /// A regular key: one typetag blob.
    Plain { blob: u128 },
    /// A projection: the projection's typetag blob plus the base key's
    /// record ordinal (projections sort after all plain records, so the base
    /// always resolves in a single forward pass).
    Projection { proj_blob: u128, base: u64 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Record {
    Occupied {
        key: PersistedKey,
        /// Ordinals into the record list. `u64::MAX` = the dep could not be
        /// persisted; the loader drops this record (dep will recompute, and
        /// this node will follow via CheckDeps).
        deps: Vec<u64>,
        verified_ranges: PersistedVersionRanges,
        dirtied_history: PersistedForceDirty,
        value_blob: u128,
    },
    Injected {
        key: PersistedKey,
        first_valid_version: u64,
        /// Wire form of the key's `InvalidationSourcePriority`.
        priority: u8,
        value_blob: u128,
    },
    /// Key interned for dep-edge resolution, but no node installed (its
    /// value could not be serialized, or its type is denylisted). Dependents
    /// stay alive: the dep recomputes on demand and equality-based reuse may
    /// still rescue the subtree above it.
    KeyOnly { key: PersistedKey },
}

impl Record {
    fn key(&self) -> &PersistedKey {
        match self {
            Record::Occupied { key, .. }
            | Record::Injected { key, .. }
            | Record::KeyOnly { key } => key,
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

type FinishedMap = DashMap<usize, Arc<pagable::storage::traits::ArcSerSlot>>;

/// Serialize one typetag-tagged dyn blob into the store. Returns None if the
/// type's fields lack Pagable impls (treated as unpersistable, not fatal).
fn store_tagged_blob<T: PagableSerialize + ?Sized>(
    storage: &DiceStorage,
    value: &T,
    finished: &FinishedMap,
) -> Option<pagable::DataKey> {
    let session_context = storage.storage().session_context();
    let mut ser = SerializerForPaging::new(session_context);
    if value.pagable_serialize(&mut ser).is_err() {
        return None;
    }
    let (data, arcs) = ser.finish();
    storage
        .storage()
        .page_out_item(data, arcs, finished, session_context)
        .ok()
}

/// The saver's view of one node's key: plain blob, projection parts, or
/// unpersistable.
enum SavedKey {
    Plain(pagable::DataKey),
    /// Base still a live DiceKey here; resolved to an ordinal later.
    Projection(pagable::DataKey, DiceKey),
    Unpersistable,
}

fn save_key(storage: &DiceStorage, key: &DiceKeyErased, finished: &FinishedMap) -> SavedKey {
    match key {
        DiceKeyErased::Key(k) => {
            match store_tagged_blob::<dyn crate::impls::key::DiceKeyDyn>(
                storage,
                k.as_ref(),
                finished,
            ) {
                Some(dk) => SavedKey::Plain(dk),
                None => SavedKey::Unpersistable,
            }
        }
        DiceKeyErased::Projection(p) => {
            match store_tagged_blob::<dyn crate::impls::key::DiceProjectionDyn>(
                storage,
                p.proj_arc().as_ref(),
                finished,
            ) {
                Some(dk) => SavedKey::Projection(dk, p.base()),
                None => SavedKey::Unpersistable,
            }
        }
    }
}

/// Save the current graph as a snapshot: values into the `DiceStorage`
/// backend, skeleton into `meta_path`. Caller must ensure DICE is idle and
/// should run `Dice::page_out()` first so most values already have
/// `DataKey`s.
///
/// `deny_key_types` (matched against `key_type_name`) forces key-only
/// persistence for values that would serialize but not survive
/// deserialization (e.g. buck2's `EvalImportKey`: frozen starlark modules
/// serialize with skipped native-function slots that panic on hydrate).
pub(crate) async fn save_snapshot(
    state: &CoreStateHandle,
    key_index: &DiceKeyIndex,
    storage: &DiceStorage,
    meta_path: &Path,
    inputs_digest: [u8; 32],
    deny_key_types: &std::collections::HashSet<String>,
) -> anyhow::Result<PersistStats> {
    let (version, extracts) = state.persist_extract().await;
    let mut stats = PersistStats::default();
    let finished = FinishedMap::new();

    // Serialize keys and values; projections are separated because their
    // canonical position depends on their base's ordinal.
    struct Staged {
        dice_key: DiceKey,
        saved_key: SavedKey,
        body: StagedBody,
    }
    enum StagedBody {
        Occupied {
            deps: Vec<DiceKey>,
            verified_ranges: PersistedVersionRanges,
            dirtied_history: PersistedForceDirty,
            value_blob: Option<u128>,
        },
        Injected {
            first_valid_version: u64,
            priority: u8,
            value_blob: u128,
        },
    }

    let mut staged: Vec<Staged> = Vec::with_capacity(extracts.len());
    for extract in &extracts {
        match extract {
            PersistNodeExtract::Occupied {
                key,
                deps,
                verified_ranges,
                dirtied_history,
                value,
            } => {
                let key_erased = key_index.get(*key);
                let saved_key = save_key(storage, key_erased, &finished);
                if matches!(saved_key, SavedKey::Unpersistable) {
                    stats.dropped_unserializable += 1;
                    continue;
                }
                let denied = deny_key_types.contains(key_erased.key_type_name());
                let value_blob = if denied {
                    None
                } else {
                    match value {
                        PersistValueExtract::Paged(dk) => Some(dk.0),
                        PersistValueExtract::Hydrated(v) => storage
                            .store_value_blob(key_erased, v.dupe(), &finished)?
                            .map(|dk| dk.0),
                    }
                };
                staged.push(Staged {
                    dice_key: *key,
                    saved_key,
                    body: StagedBody::Occupied {
                        deps: deps.clone(),
                        verified_ranges: PersistedVersionRanges::from_internal(verified_ranges),
                        dirtied_history: PersistedForceDirty::from_internal(dirtied_history),
                        value_blob,
                    },
                });
            }
            PersistNodeExtract::Injected {
                key,
                first_valid_version,
                value,
            } => {
                let key_erased = key_index.get(*key);
                let saved_key = save_key(storage, key_erased, &finished);
                let denied = deny_key_types.contains(key_erased.key_type_name());
                let value_blob = if denied {
                    None
                } else {
                    storage
                        .store_value_blob(key_erased, value.dupe(), &finished)?
                        .map(|dk| dk.0)
                };
                match (&saved_key, value_blob) {
                    (SavedKey::Plain(_), Some(vb)) => {
                        staged.push(Staged {
                            dice_key: *key,
                            saved_key,
                            body: StagedBody::Injected {
                                first_valid_version: first_valid_version.value() as u64,
                                priority: priority_to_wire(
                                    key_erased.invalidation_source_priority(),
                                ),
                                value_blob: vb,
                            },
                        });
                    }
                    _ => {
                        // An injected leaf without its value is useless (it
                        // is the diff baseline); drop it. Its dependents
                        // survive as records but drop at load via dangling
                        // deps - correct, and accounted.
                        stats.dropped_unserializable += 1;
                    }
                }
            }
        }
    }

    // Canonical order: plain records sorted by (kind, key blob) first, then
    // projections by (proj blob, base ordinal). Deterministic for identical
    // graphs regardless of HashMap iteration order.
    fn plain_rank(s: &Staged) -> Option<(u8, u128)> {
        match (&s.saved_key, &s.body) {
            (SavedKey::Plain(dk), StagedBody::Occupied { value_blob, .. }) => {
                Some((if value_blob.is_some() { 0 } else { 2 }, dk.0))
            }
            (SavedKey::Plain(dk), StagedBody::Injected { .. }) => Some((1, dk.0)),
            (SavedKey::Projection(..), _) => None,
            (SavedKey::Unpersistable, _) => unreachable!("filtered above"),
        }
    }
    let (mut plains, mut projections): (Vec<Staged>, Vec<Staged>) = staged
        .into_iter()
        .partition(|s| matches!(s.saved_key, SavedKey::Plain(_)));
    plains.sort_by_key(|s| plain_rank(s).unwrap());

    let mut ordinal_of: std::collections::HashMap<DiceKey, u64> = std::collections::HashMap::new();
    for (i, s) in plains.iter().enumerate() {
        ordinal_of.insert(s.dice_key, i as u64);
    }
    // Projections whose base did not survive are dropped here.
    projections.retain(|s| match &s.saved_key {
        SavedKey::Projection(_, base) => {
            let keep = ordinal_of.contains_key(base);
            if !keep {
                stats.dropped_dangling_dep += 1;
            }
            keep
        }
        _ => unreachable!(),
    });
    projections.sort_by_key(|s| match &s.saved_key {
        SavedKey::Projection(dk, base) => (dk.0, ordinal_of[base]),
        _ => unreachable!(),
    });
    for (i, s) in projections.iter().enumerate() {
        ordinal_of.insert(s.dice_key, (plains.len() + i) as u64);
    }

    let all: Vec<Staged> = plains.into_iter().chain(projections).collect();
    let mut records: Vec<Record> = Vec::with_capacity(all.len());
    for s in &all {
        let key = match &s.saved_key {
            SavedKey::Plain(dk) => PersistedKey::Plain { blob: dk.0 },
            SavedKey::Projection(dk, base) => PersistedKey::Projection {
                proj_blob: dk.0,
                base: ordinal_of[base],
            },
            SavedKey::Unpersistable => unreachable!(),
        };
        match &s.body {
            StagedBody::Occupied {
                deps,
                verified_ranges,
                dirtied_history,
                value_blob,
            } => match value_blob {
                Some(vb) => {
                    records.push(Record::Occupied {
                        key,
                        deps: deps
                            .iter()
                            .map(|d| ordinal_of.get(d).copied().unwrap_or(u64::MAX))
                            .collect(),
                        verified_ranges: verified_ranges.clone(),
                        dirtied_history: dirtied_history.clone(),
                        value_blob: *vb,
                    });
                    stats.nodes_persisted += 1;
                }
                None => {
                    records.push(Record::KeyOnly { key });
                    stats.keys_only += 1;
                }
            },
            StagedBody::Injected {
                first_valid_version,
                priority,
                value_blob,
            } => {
                records.push(Record::Injected {
                    key,
                    first_valid_version: *first_valid_version,
                    priority: *priority,
                    value_blob: *value_blob,
                });
                stats.nodes_injected += 1;
            }
        }
    }

    storage.storage().flush()?;

    let meta = MetaFile {
        header: SnapshotHeader::new(inputs_digest, records.len() as u64, version.value() as u64),
        records,
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

    // Pass 1: fetch + deserialize + intern every key. Plain keys resolve
    // directly; projections resolve against earlier records (they always
    // sort after every plain record, and after any projection they base on).
    let handle = PagableStorageHandle::new(storage.storage().dupe());
    let mut dice_keys: Vec<Option<DiceKey>> = Vec::with_capacity(meta.records.len());
    let mut erased_keys: Vec<Option<DiceKeyErased>> = Vec::with_capacity(meta.records.len());
    for record in &meta.records {
        let loaded = match record.key() {
            PersistedKey::Plain { blob } => {
                match fetch_and_deserialize::<dyn crate::impls::key::DiceKeyDyn>(
                    storage,
                    &handle,
                    pagable::DataKey(*blob),
                )
                .await
                {
                    Some(boxed) => Some(DiceKeyErased::Key(Arc::from(boxed))),
                    None => None,
                }
            }
            PersistedKey::Projection { proj_blob, base } => {
                let base_key = usize::try_from(*base)
                    .ok()
                    .and_then(|ix| dice_keys.get(ix).copied().flatten());
                match (
                    base_key,
                    fetch_and_deserialize::<dyn crate::impls::key::DiceProjectionDyn>(
                        storage,
                        &handle,
                        pagable::DataKey(*proj_blob),
                    )
                    .await,
                ) {
                    (Some(bk), Some(boxed)) => Some(DiceKeyErased::Projection(
                        crate::impls::key::ProjectionWithBase::from_persisted_parts(
                            bk,
                            Arc::from(boxed),
                        ),
                    )),
                    _ => None,
                }
            }
        };
        match loaded {
            Some(erased) => {
                let dice_key = key_index.index(CowDiceKeyHashed::from_erased(erased.dupe()));
                dice_keys.push(Some(dice_key));
                erased_keys.push(Some(erased));
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

async fn fetch_and_deserialize<T: ?Sized>(
    storage: &DiceStorage,
    handle: &PagableStorageHandle,
    data_key: pagable::DataKey,
) -> Option<Box<T>>
where
    for<'de> T: PagableBoxDeserialize<'de>,
{
    let data = storage.storage().fetch_data(&data_key).await.ok()?;
    let mut deser = PagableDeserializerImpl::new(&data.data, &data.arcs, handle);
    T::deserialize_box(&mut deser).ok()
}
