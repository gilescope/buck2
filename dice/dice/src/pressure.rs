/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! Memory-pressure eviction: page cold values out mid-build.
//!
//! Fork addition, kept out of the upstream files it extends so their impl
//! blocks stay untouched by merges. Same crate, so these are plain inherent
//! impls - call sites are unchanged.

use std::sync::Arc as StdArc;

use dupe::Dupe;
use futures::Future;
use tokio::sync::oneshot;
use tokio::sync::oneshot::Sender;

use crate::core::graph::nodes::VersionedGraphNode;
use crate::core::internals::CoreState;
use crate::core::state::CoreStateHandle;
use crate::core::state::StateRequest;
use crate::dice::Dice;
use crate::epoch::cache::SharedCache;
use crate::key::DiceKey;
use crate::value::DiceValidValue;
use crate::versions::VersionNumber;

impl Dice {
    /// Targeted eviction for memory pressure: page out cold values while
    /// builds may still be running - unlike [`Dice::page_out`], no idle
    /// requirement. Later lookups of an evicted key hydrate on demand.
    ///
    /// Candidates exclude values referenced by any active transaction's cache
    /// (the cache pins the `Arc`, so evicting those frees nothing) and values
    /// with an rdep task still pending (likely read straight back - thrash).
    /// Coldest first, ranked by last-verified version. Only key types named in
    /// `allowed_key_types` (matched against `Key::key_type_name`, e.g.
    /// `"AnalysisKey"`) are eligible.
    ///
    /// `max_values` bounds how many values this call evicts. It is a count,
    /// not bytes, deliberately: per-value resident size is not reliably
    /// measurable before L2 (values share arenas behind `Arc`s, so both
    /// unique-ownership walks and serialized size mis-state what an eviction
    /// frees). The caller's watermark loop is the controller - evict a chunk,
    /// purge the allocator, re-measure RSS, repeat until under the low
    /// watermark. Values already serialized by an earlier page-out are
    /// evicted without re-serialization.
    ///
    /// All evictions have been applied on the core-state thread by the time
    /// this returns. No-op without pagable storage.
    pub async fn evict_under_pressure(
        self: &StdArc<Self>,
        max_values: usize,
        allowed_key_types: &std::collections::HashSet<String>,
    ) -> anyhow::Result<PressureEvictStats> {
        let Some(storage) = self.pagable_storage.as_ref() else {
            return Ok(PressureEvictStats::default());
        };

        // Scan the active transactions' caches off the core-state thread -
        // they're concurrent structures, and the state thread is the build's
        // bottleneck. Slight staleness is fine (see
        // `CoreState::pressure_eviction_candidates`).
        let caches = self.state_handle.active_caches().await;
        let (referenced, pending) = tokio::task::spawn_blocking(move || {
            let mut referenced = crate::HashSet::default();
            let mut pending = crate::HashSet::default();
            for cache in &caches {
                cache.collect_referenced_keys(&mut referenced, &mut pending);
            }
            (referenced, pending)
        })
        .await?;
        let mut candidates = self
            .state_handle
            .pressure_candidates(referenced, pending)
            .await;
        candidates
            .retain(|c| allowed_key_types.contains(self.key_index.get(c.key).key_type_name()));
        let candidate_count = candidates.len();
        candidates.sort_by_key(|c| c.last_verified_begin);
        candidates.truncate(max_values);

        // Every candidate needs serializing: a candidate is a value that has
        // never been paged out, so none of them have bytes on disk already.
        let mut to_serialize: Vec<_> = candidates.into_iter().map(|c| (c.key, c.value)).collect();

        let stats = PressureEvictStats {
            candidates: candidate_count,
            selected: to_serialize.len(),
        };
        // Serialize in small sub-batches: page-out transiently buffers each
        // value's serialized bytes (blob slots + backend WAL), and under
        // pressure that transient must stay bounded - a single sweep paging
        // out 2.5GB of values spiked RSS 3.3GB ABOVE the no-eviction
        // baseline. Each page_out call flushes and releases its buffers, so
        // the graph frees progressively as batches complete. Cost: shared
        // subtrees re-serialize across batches (the store dedups them at
        // rest) - CPU spent to keep the memory envelope flat.
        const SERIALIZE_BATCH: usize = 256;
        while !to_serialize.is_empty() {
            let split = to_serialize.len().min(SERIALIZE_BATCH);
            let batch: Vec<_> = to_serialize.drain(..split).collect();
            storage
                .page_out(batch, &self.key_index, &self.state_handle, || false)
                .await?;
        }
        // Drain the core-state FIFO so every eviction message has been
        // processed before we report back (callers purge the allocator next).
        // Any async round-trip works as the barrier; this is the cheapest.
        let _ = self.state_handle.current_version().await;
        Ok(stats)
    }
}

/// Result summary of [`Dice::evict_under_pressure`]. Counts are of *attempted*
/// evictions - a value recomputed mid-flight is skipped by the checked
/// eviction on the state thread and stays resident. RSS is the ground truth;
/// these numbers only steer the caller's hysteresis loop.
#[derive(Debug, Default)]
pub struct PressureEvictStats {
    /// Eligible cold values before the byte target cut selection off.
    pub candidates: usize,
    /// Values selected for eviction this call.
    pub selected: usize,
}
/// One node eligible for pressure eviction, from
/// `CoreState::pressure_eviction_candidates`. The value is an `Arc` dupe; the
/// caller sizes/serializes it off the state thread.
pub(crate) struct PressureCandidate {
    pub(crate) key: DiceKey,
    pub(crate) value: DiceValidValue,
    /// Coldness rank (older = colder); see `OccupiedGraphNode::last_verified_begin`.
    pub(crate) last_verified_begin: VersionNumber,
}

impl CoreState {
    /// The active transactions' caches, for off-state-thread inspection (the
    /// cache structures are concurrent). Cheap: clones of `Arc`d handles.
    pub(crate) fn active_caches(&self) -> Vec<SharedCache> {
        self.version_tracker
            .currently_active()
            .map(|(_refcount, cache)| cache.dupe())
            .collect()
    }

    /// Nodes eligible for pressure eviction: occupied, hydrated, not in
    /// `referenced` (keys referenced by an active transaction's cache - the
    /// cache pins the value's `Arc`, so evicting those frees nothing), and
    /// with no rdep in `pending` (an in-flight parent may read the value
    /// straight back - thrash). The caller builds both sets off this thread
    /// from `active_caches()` - scanning the caches here would stall the
    /// core-state thread, which is the build's bottleneck (measured: +75%
    /// wall on an analysis-heavy leg at a 5s poll). The sets may be slightly
    /// stale; evicting a just-completed value is harmless (it stays pinned by
    /// its cache until the transaction drops, then hydrates on demand).
    pub(crate) fn pressure_eviction_candidates(
        &self,
        referenced: &crate::HashSet<DiceKey>,
        pending: &crate::HashSet<DiceKey>,
    ) -> Vec<PressureCandidate> {
        // Enumerated from the graph's page-out candidate set (resident and never
        // paged out), same source as `keys_to_page_out`, rather than scanning
        // every node.
        self.graph
            .page_out_candidates()
            .iter()
            .filter_map(|index| {
                let key = DiceKey {
                    index: index as u32,
                };
                let VersionedGraphNode::Occupied(occ) = self.graph.nodes().get(&key)? else {
                    return None;
                };
                let value = occ.val().as_hydrated()?;
                if referenced.contains(&key) {
                    return None;
                }
                if occ.rdeps().any(|r| pending.contains(&r)) {
                    return None;
                }
                Some(PressureCandidate {
                    key,
                    value: value.dupe(),
                    last_verified_begin: occ.last_verified_begin(),
                })
            })
            .collect()
    }
}

/// The fork's core-state requests, carried as a single [`StateRequest`]
/// variant so `state.rs` and the processor's match each grow one line rather
/// than one per request.
pub(crate) enum PressureRequest {
    /// The active transactions' caches, for off-thread scanning.
    ActiveCaches { resp: Sender<Vec<SharedCache>> },
    /// Collect nodes eligible for pressure eviction.
    Candidates {
        referenced: crate::HashSet<DiceKey>,
        pending: crate::HashSet<DiceKey>,
        resp: Sender<Vec<PressureCandidate>>,
    },
}

impl PressureRequest {
    /// Run on the core-state thread. Dropped senders mean the caller went away.
    pub(crate) fn handle(self, state: &mut CoreState) {
        match self {
            PressureRequest::ActiveCaches { resp } => drop(resp.send(state.active_caches())),
            PressureRequest::Candidates {
                referenced,
                pending,
                resp,
            } => drop(resp.send(state.pressure_eviction_candidates(&referenced, &pending))),
        }
    }
}

impl CoreStateHandle {
    /// The active transactions' caches, cloned for off-thread scanning.
    pub(crate) fn active_caches(&self) -> impl Future<Output = Vec<SharedCache>> + use<> {
        let (resp, recv) = oneshot::channel();
        self.call(
            StateRequest::Pressure(PressureRequest::ActiveCaches { resp }),
            recv,
        )
    }

    /// Collect nodes eligible for pressure eviction (hydrated, unpinned, no
    /// pending rdep task). `referenced`/`pending` are prebuilt off-thread from
    /// [`active_caches`](Self::active_caches).
    pub(crate) fn pressure_candidates(
        &self,
        referenced: crate::HashSet<DiceKey>,
        pending: crate::HashSet<DiceKey>,
    ) -> impl Future<Output = Vec<PressureCandidate>> + use<> {
        let (resp, recv) = oneshot::channel();
        self.call(
            StateRequest::Pressure(PressureRequest::Candidates {
                referenced,
                pending,
                resp,
            }),
            recv,
        )
    }
}
